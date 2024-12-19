import sys
import time
import threading
import requests
from loguru import logger
from flask import Flask, request, jsonify

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss} | <level>{message}</level></green>",
           colorize=True, enqueue=True)

HEARTBEAT_TIMEOUT = 5
ELECTION_TIMEOUT = 10
MIN_SUSPECT_TIMEOUT = 15
MAX_SUSPECT_TIMEOUT = 20

current_replica_urls = [
    "http://127.0.0.1:8001/",
    "http://127.0.0.1:8002/",
    "http://127.0.0.1:8003/",
    "http://127.0.0.1:8004/",
]

PLACEHOLDER_LOG = [{"key": "placeholder_key", "value": "placeholder_value"}]

class RaftNode:
    def __init__(self, node_url):
        self.node_url = node_url
        self.current_term = 0
        self.voted_for = None
        self.log = PLACEHOLDER_LOG.copy()
        self.commit_index = -1
        self.last_applied = -1
        self.role = "follower"  # Possible roles: follower, candidate, leader
        self.leader_url = None
        self.votes_received = 0
        self.setup_timers()

    def setup_timers(self):
        self.election_timer = threading.Timer(ELECTION_TIMEOUT, self.suspect_leader_has_failed_or_on_election_timeout)
        self.heartbeat_timer = threading.Timer(HEARTBEAT_TIMEOUT, self.heartbeat)
        self.reset_election_timer()

    def reset_election_timer(self):
        self.election_timer.cancel()
        self.election_timer = threading.Timer(ELECTION_TIMEOUT, self.suspect_leader_has_failed_or_on_election_timeout)
        self.election_timer.start()

    def suspect_leader_has_failed_or_on_election_timeout(self):
        logger.info(f"{self.node_url} suspects the leader has failed. Starting election.")
        self.start_election()

    def start_election(self):
        self.current_term += 1
        self.voted_for = None
        self.votes_received = 0
        available_urls = [url for url in current_replica_urls if self.check_node_availability(url)]
        logger.info(f"{self.node_url} found available nodes: {available_urls}")

        if available_urls and available_urls[0] == self.node_url:
            # If the current node is the first available, vote for itself and become leader immediately
            self.voted_for = self.node_url
            logger.info(f"{self.node_url} is voting for itself and becoming leader.")
            self.become_leader()
        else:
            for url in available_urls:
                if url != self.node_url:
                    try:
                        response = requests.post(url + "vote_request", json={"term": self.current_term, "candidate": self.node_url}, timeout=5)
                        if response.status_code == 200 and response.json().get("vote_granted"):
                            self.votes_received += 1
                            logger.info(f"{self.node_url} received vote from {url}")
                            self.check_for_majority_votes()
                        else:
                            logger.warning(f"Vote denied by {url} or invalid response.")
                    except requests.RequestException as e:
                        logger.warning(f"Failed to contact {url} during election: {e}")

    def check_node_availability(self, url):
        try:
            response = requests.get(url + "health", timeout=3)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def check_for_majority_votes(self):
        if self.votes_received > len(current_replica_urls) // 2:
            self.become_leader()

    def on_receiving_vote_request(self, term, candidate):
        if term > self.current_term or (term == self.current_term and (self.voted_for is None or self.voted_for == candidate)):
            self.current_term = term
            self.voted_for = candidate
            self.reset_election_timer()
            return True
        return False

    def become_leader(self):
        logger.info(f"{self.node_url} is now the leader.")
        self.role = "leader"
        self.leader_url = self.node_url
        if self.log == PLACEHOLDER_LOG:
            logger.info(f"{self.node_url} resetting placeholder log to empty.")
            self.log = []
        self.broadcast("heartbeat", {})
        self.heartbeat()

    def remove_leadership(self):
        self.role = "follower"
        self.leader_url = None
        self.reset_election_timer()

    def heartbeat(self):
        if self.role == "leader":
            logger.info(f"{self.node_url} sending heartbeat.")
            self.broadcast("heartbeat", {"leader": self.node_url})
            self.heartbeat_timer = threading.Timer(HEARTBEAT_TIMEOUT, self.heartbeat)
            self.heartbeat_timer.start()

    def broadcast(self, message_type, message_data):
        logger.info(f"{self.node_url} broadcasting {message_type} to replicas.")
        for url in current_replica_urls:
            if url != self.node_url:
                self.send_message_to_replica(url, message_type, message_data)

    def send_message_to_replica(self, url, message_type, message_data):
        logger.debug(f"Sending {message_type} to {url} with data: {message_data}")
        try:
            response = requests.post(url + message_type, json=message_data, timeout=5)
            logger.debug(f"Response from {url}: {response.status_code} {response.text}")
        except requests.RequestException as e:
            logger.error(f"Failed to send {message_type} to {url}: {e}")

    def append_entries(self, entries, replicate=True):
        for entry in entries:
            if entry not in self.log:
                self.log.append(entry)
                logger.info(f"{self.node_url} appended entry: {entry}")
        if replicate and self.role == "leader":
            self.replicate_log(entries)

    def replicate_log(self, entries):
        logger.info(f"{self.node_url} replicating log entries to replicas.")
        for url in current_replica_urls:
            if url != self.node_url:
                try:
                    response = requests.post(url + "replicate_log", json={"entries": entries}, timeout=5)
                    if response.status_code == 200:
                        logger.debug(f"Log replicated successfully to {url}")
                    else:
                        logger.error(f"Failed to replicate log to {url}: {response.status_code} {response.text}")
                except requests.RequestException as e:
                    logger.error(f"Failed to replicate log to {url}: {e}")

    def request_log_from_leader(self):
        if self.leader_url:
            logger.info(f"{self.node_url} requesting log from leader {self.leader_url}")
            try:
                response = requests.get(self.leader_url + "sync_log", timeout=5)
                if response.status_code == 200:
                    self.log = response.json().get("entries", [])
                    logger.info(f"{self.node_url} synchronized log from leader: {self.log}")
                else:
                    logger.error(f"Failed to get log from leader: {response.status_code} {response.text}")
            except requests.RequestException as e:
                logger.error(f"Failed to request log from leader: {e}")

class RaftWebServer(RaftNode):
    def __init__(self, node_url):
        super().__init__(node_url)
        self.app = Flask(__name__)
        self.setup_routes()

    def setup_routes(self):
        @self.app.route("/read/<key>", methods=["GET"])
        def read_record(key):
            for entry in self.log:
                if entry.get("key") == key:
                    return jsonify(entry), 200
            return jsonify({"error": "Key not found"}), 404

        @self.app.route("/write", methods=["POST"])
        def set_record():
            data = request.json
            if not data or "key" not in data or "value" not in data:
                return jsonify({"error": "Invalid data format. 'key' and 'value' are required."}), 400

            if self.role != "leader":
                if self.leader_url:
                    logger.info(f"Forwarding write request to leader {self.leader_url}")
                    try:
                        response = requests.post(self.leader_url + "write", json=data, timeout=5)
                        self.leader_url = response.headers.get("Location", self.leader_url)
                        return jsonify(response.json()), response.status_code
                    except requests.RequestException as e:
                        logger.error(f"Failed to forward write request to leader: {e}")
                        return jsonify({"error": "Leader unavailable."}), 503
                else:
                    logger.warning("Write request rejected: No leader available.")
                    return jsonify({"error": "No leader available to handle the request."}), 503

            self.append_entries([data])
            return jsonify({"status": "OK"}), 201

        @self.app.route("/delete/<key>", methods=["DELETE"])
        def delete_record(key):
            if self.role != "leader":
                if self.leader_url:
                    logger.info(f"Forwarding delete request to leader {self.leader_url}")
                    try:
                        response = requests.delete(self.leader_url + f"delete/{key}", timeout=5)
                        return jsonify(response.json()), response.status_code
                    except requests.RequestException as e:
                        logger.error(f"Failed to forward delete request to leader: {e}")
                        return jsonify({"error": "Leader unavailable."}), 503
                else:
                    logger.warning("Delete request rejected: No leader available.")
                    return jsonify({"error": "No leader available to handle the request."}), 503

            self.log = [entry for entry in self.log if entry.get("key") != key]
            return jsonify({"status": "Deleted"}), 200

        @self.app.route("/cas", methods=["PUT"])
        def cas_record():
            data = request.json
            if not data or "key" not in data or "old_value" not in data or "new_value" not in data:
                return jsonify({"error": "Invalid data format. 'key', 'old_value', and 'new_value' are required."}), 400

            if self.role != "leader":
                if self.leader_url:
                    logger.info(f"Forwarding CAS request to leader {self.leader_url}")
                    try:
                        response = requests.put(self.leader_url + "cas", json=data, timeout=5)
                        return jsonify(response.json()), response.status_code
                    except requests.RequestException as e:
                        logger.error(f"Failed to forward CAS request to leader: {e}")
                        return jsonify({"error": "Leader unavailable."}), 503
                else:
                    logger.warning("CAS request rejected: No leader available.")
                    return jsonify({"error": "No leader available to handle the request."}), 503

            key = data.get("key")
            old_value = data.get("old_value")
            new_value = data.get("new_value")

            for entry in self.log:
                if entry.get("key") == key and entry.get("value") == old_value:
                    entry["value"] = new_value
                    logger.info(f"CAS succeeded: key={key}, old_value={old_value}, new_value={new_value}")
                    return jsonify({"status": "OK"}), 200

            logger.error(f"CAS failed: key={key}, old_value={old_value}, requested_new_value={new_value}")
            return jsonify({"error": "CAS failed"}), 400

        @self.app.route("/vote_request", methods=["POST"])
        def handle_vote_request():
            data = request.json
            if not data or "term" not in data or "candidate" not in data:
                return jsonify({"error": "Invalid vote request format."}), 400

            term = data["term"]
            candidate = data["candidate"]
            vote_granted = self.on_receiving_vote_request(term, candidate)
            return jsonify({"vote_granted": vote_granted}), 200

        @self.app.route("/heartbeat", methods=["POST"])
        def handle_heartbeat():
            data = request.json
            if not data or "leader" not in data:
                return jsonify({"error": "Invalid heartbeat format."}), 400

            leader = data["leader"]
            self.leader_url = leader
            self.reset_election_timer()

            # If log is still placeholder, request full log from leader
            if self.log == PLACEHOLDER_LOG:
                self.request_log_from_leader()

            logger.info(f"{self.node_url} received heartbeat from leader: {leader}")
            return jsonify({"status": "OK"}), 200

        @self.app.route("/replicate_log", methods=["POST"])
        def handle_replicate_log():
            data = request.json
            if not data or "entries" not in data:
                return jsonify({"error": "Invalid log replication format."}), 400

            entries = data["entries"]
            self.append_entries(entries, replicate=False)
            logger.info(f"{self.node_url} replicated entries: {entries}")
            return jsonify({"status": "OK"}), 200

        @self.app.route("/sync_log", methods=["GET"])
        def sync_log():
            return jsonify({"entries": self.log}), 200

        @self.app.route("/health", methods=["GET"])
        def health_check():
            return jsonify({"status": "OK"}), 200

    def run(self):
        port = int(self.node_url.split(":")[-1][:-1])
        self.app.run(port=port)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python main.py <replica_index>")
        sys.exit(1)

    try:
        replica_index = int(sys.argv[1])
        node_url = current_replica_urls[replica_index]
        node = RaftWebServer(node_url)
        node.run()
    except (IndexError, ValueError):
        logger.error("Invalid replica index. Please provide an integer between 0 and {len(current_replica_urls) - 1}.")
        sys.exit(1)

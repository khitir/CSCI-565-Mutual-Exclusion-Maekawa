import socket
import threading
from math import sqrt, ceil


class VectorClock:
    def __init__(self, num_processes, process_id):
        # Initialize vector clock for a process with a list of zeroes, one for each process
        self.clock = [0] * num_processes
        self.process_id = process_id  # Unique identifier for the process

    def increment(self):
        # Increment the vector clock value for the local process
        self.clock[self.process_id] += 1

    def update(self, other_clock):
        # Update the vector clock by taking element-wise maximum with another clock
        for i in range(len(self.clock)):
            self.clock[i] = max(self.clock[i], other_clock[i])
        # After receiving a message, increment the local clock to reflect causality
        self.increment()

    def get_timestamp(self):
        # Return the current state of the vector clock
        return self.clock

    def __str__(self):
        # String representation of the vector clock for printing
        return f"Process {self.process_id}: {self.clock}"


class MaekawaMutex:
    def __init__(self, num_processes):
        # Initialize Maekawa's mutual exclusion algorithm variables
        self.num_processes = num_processes
        self.process_id = None  # This process's unique ID
        self.hosts = None  # List of hosts (IP and port) of all processes
        self.voting_group = None  # Voting group for this process
        self.vector_clock = None  # Vector clock to track event ordering
        self.lock_granted = False  # Whether this process currently holds the lock
        self.request_queue = []  # Queue for holding pending requests
        self.votes_received = 0  # Count of votes received for lock acquisition
        self.server_socket = None  # Socket for server to listen for messages

    def GlobalInitialize(self, thisHost, hosts):
        # Set process ID and list of hosts, then start the server to listen for incoming connections
        self.process_id = thisHost
        self.hosts = hosts
        self.start_server()

    def MInitialize(self, votingGroupHosts):
        # Initialize the voting group and reset the lock state and vector clock
        self.voting_group = votingGroupHosts
        self.votes_received = 0
        self.lock_granted = False
        self.request_queue = []
        self.vector_clock = VectorClock(self.num_processes, self.process_id)

    def MLockMutex(self):
        # Request to acquire the lock by voting for itself and multicasting the request
        print(f"Process {self.process_id} requesting lock...")
        self.receive_ok()  # Self-voting for the lock
        self.multicast_request()  # Send lock request to voting group

    def MReleaseMutex(self):
        # Release the lock and notify other processes
        print(f"Process {self.process_id} releasing lock and exiting critical section")
        self.multicast_release()  # Multicast release message to voting group
        self.lock_granted = False
        self.votes_received = 0
        # If there are requests in the queue, send OK to the next requester
        if self.request_queue:
            next_requestor = self.request_queue.pop(0)
            self.send_message(self.hosts[next_requestor], f"{self.process_id}|OK|{self.vector_clock.get_timestamp()}")

    def MCleanup(self):
        # Cleanup the state of the mutex variables
        self.lock_granted = False
        self.votes_received = 0
        self.request_queue = []
        print(f"Process {self.process_id} cleaned up")

    def QuitAndCleanup(self):
        # Close the server socket and clean up
        if self.server_socket:
            self.server_socket.close()
        print(f"Process {self.process_id} shutting down")

    # Server setup and message handling
    def start_server(self):
        # Setup the server socket to listen for incoming connections
        host = self.hosts[self.process_id]
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(host)  # Bind to the specified IP and port
        self.server_socket.listen(5)  # Allow up to 5 pending connections
        print(f"Process {self.process_id} listening on {host}")
        # Start a thread to handle incoming messages concurrently
        threading.Thread(target=self.listen_for_messages, daemon=True).start()

    def listen_for_messages(self):
        # Continuously listen for incoming messages
        while True:
            client_socket, addr = self.server_socket.accept()  # Accept an incoming connection
            message = client_socket.recv(1024).decode()  # Receive the message up to 1024 bytes.
            self.handle_message(message)  # Handle the received message
            client_socket.close()  # Close the connection

    def send_message(self, host, message):
        # Send a message to a specified host (IP and port)
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print(f"Process {self.process_id} trying to connect to {host} to send {message}")
            client_socket.connect(host)  # Connect to the host
            client_socket.sendall(message.encode())  # Send the message
            client_socket.close()  # Close the connection
        except Exception as e:
            print(f"Failed to send message to {host}: {e}")

    def handle_message(self, message):
        # Parse the received message and handle it based on the message type
        sender_id, msg_type, timestamp = message.split('|')
        sender_id = int(sender_id)
        timestamp = eval(timestamp)  # Convert the timestamp string back to a list

        # Update the vector clock with the received timestamp
        self.vector_clock.update(timestamp)
        print(self.vector_clock)

        # Handle the message based on its type (REQUEST, OK, RELEASE)
        if msg_type == "REQUEST":
            self.receive_request(sender_id)
        elif msg_type == "OK":
            self.receive_ok()
        elif msg_type == "RELEASE":
            self.receive_release(sender_id)

    # Multicast to all in voting group
    def multicast_request(self):
        # Increment the vector clock before sending the request
        self.vector_clock.increment()
        # Send a REQUEST message to all members in the voting group
        for member in self.voting_group:
            if member != self.process_id:  # Skip sending to itself
                self.send_message(self.hosts[member], f"{self.process_id}|REQUEST|{self.vector_clock.get_timestamp()}")

    def multicast_release(self):
        # Increment the vector clock before sending the release message
        self.vector_clock.increment()
        # Send a RELEASE message to all members in the voting group
        for member in self.voting_group:
            self.send_message(self.hosts[member], f"{self.process_id}|RELEASE|{self.vector_clock.get_timestamp()}")

    def receive_request(self, requester_id):
        # Handle an incoming REQUEST message
        print(f"Process {self.process_id} received a REQUEST from Process {requester_id}")
        if self.lock_granted:
            # If the lock is currently granted, add the requester to the queue
            self.request_queue.append(requester_id)
        else:
            # Grant the lock to the requester and send an OK message
            self.lock_granted = True
            self.send_message(self.hosts[requester_id], f"{self.process_id}|OK|{self.vector_clock.get_timestamp()}")
            print(f"Process {self.process_id} sent OK to Process {requester_id}")

    def receive_ok(self):
        # Handle an incoming OK message (vote)
        self.votes_received += 1
        print(f"Process {self.process_id} received OK. Total votes received: {self.votes_received}/{len(self.voting_group)}")
        
        # If all votes have been received, enter the critical section
        if self.votes_received == len(self.voting_group):
            print(f"Process {self.process_id} entering critical section")
            self.MReleaseMutex()  # Exiting the critical section automatically after entering
        else:
            print(f"Process {self.process_id} is still waiting for more OK votes.")

    def receive_release(self, sender_id):
        # Handle an incoming RELEASE message
        print(f"Process {self.process_id} received RELEASE from Process {sender_id}")
        self.lock_granted = False  # Release the lock
        # If there are pending requests, send OK to the next requester
        if self.request_queue:
            next_requestor = self.request_queue.pop(0)
            self.send_message(self.hosts[next_requestor], f"{self.process_id}|OK|{self.vector_clock.get_timestamp()}")


# Generate voting groups - pairwise non-disjoint sets
def generate_voting_groups(num_processes):
    """Generate voting groups using one of the two methods from Maekawa's paper."""
    groups = []
    K = ceil(sqrt(num_processes))  # Calculates the optimal group size K based on the square root of the number of processes

    # Create the quorum set based on a grid structure (method 2 in the paper)
    for i in range(num_processes):
        group = []
        row_start = (i // K) * K # Calculates the starting index for the row in the grid that the process belongs to
        # Add members from the row into the current process's voting group
        for j in range(K):
            group.append(row_start + j)
        # Add members from the column
        for j in range(K):
            column_element = i % K + j * K
            if column_element < num_processes and column_element not in group:
                group.append(column_element)
        groups.append(group)

    return groups
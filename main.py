from distributed_mutex import MaekawaMutex, generate_voting_groups
import time
# Define the process ID and number of processes
process_id = int(input("Enter the process ID: "))
num_processes = 2  # Adjust this based on setup

# Define hosts for each process (IP and port)
hosts = [
    ('192.168.0.88', 5000),
    ('192.168.0.99', 5001)
]

# Automatically generate voting groups based on number of processes
voting_groups = generate_voting_groups(num_processes)

# Initialize MaekawaMutex
mutex = MaekawaMutex(num_processes)

# Initialize Global configuration
mutex.GlobalInitialize(process_id, hosts)

# Initialize Maekawa's algorithm
mutex.MInitialize(voting_groups[process_id])

time.sleep(5)  # Delay to allow all processes to initialize before making lock requests

# Request the lock
input("Press Enter to request lock...")
mutex.MLockMutex()

# Release the lock (this can be done after some operation or after input)
input("Press Enter to release lock...")
mutex.MReleaseMutex()

# Clean up
mutex.MCleanup()

# Shut down the process
mutex.QuitAndCleanup()

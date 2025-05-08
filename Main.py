import sys
import subprocess
import time
import signal

def start_server(port):
    # Start the server process
    cmd = ["python", "Server.py", str(port)]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

def run_clients_sequentially(port, num_clients):
    # Run the client sequentially
    for i in range(1, num_clients+1):
        cmd = ["python", "Client.py", "localhost", str(port), f"client-{i}.txt"]
        subprocess.run(cmd, check=True)

def run_clients_concurrently(port, num_clients):
    # Concurrent running of client
    processes = []
    for i in range(1, num_clients+1):
        cmd = ["python", "Client.py", "localhost", str(port), f"client-{i}.txt"]
        p = subprocess.Popen(cmd)
        processes.append(p)
    return processes

def main():
    if len(sys.argv) != 3:
        sys.exit(1)
    
    port = int(sys.argv[1])
    num_clients = int(sys.argv[2])
    
    print("Sequential execution")
    server = start_server(port)
    time.sleep(2)
    
    try:
        run_clients_sequentially(port, num_clients)
    finally:
        server.send_signal(signal.SIGINT)
        server.wait()
    
    print("Concurrent execution")
    server = start_server(port)
    time.sleep(2)
    
    try:
        processes = run_clients_concurrently(port, num_clients)
        # Waiting for all clients to complete
        for p in processes:
            p.wait()
    finally:
        server.send_signal(signal.SIGINT)
        server.wait()

if __name__ == "__main__":
    main()
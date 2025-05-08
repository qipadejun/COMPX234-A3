import socket
import threading
import time
from collections import defaultdict

class TupleSpace:
    def __init__(self):
        self.tuples = {}
        self.lock = threading.Lock()
        self.stats = {
            'total_clients': 0
            'total_operations': 0
            'total_reads': 0
            'total_gets': 0
            'total_puts': 0
            'total_errors': 0
            'key_sizes': []
            'value_sizes': []
        }
    
    # Write the put instruction
    def put(self, key, value):
        with self.lock:
            self.stats['total_operations'] += 1
            self.stats['total_puts'] += 1
            if key in self.tuples:
                self.stats['total_errors'] += 1
                return (False, "ERR {} already exists".format(key))
            else:
                self.tuples[key] = value
                self.stats['key_sizes'].append(len(key))
                self.stats['value_sizes'].append(len(value))
                return (True, "OK ({}, {}) added".format(key, value))
    
    # Write the get instruction
    def get(self, key):
        with self.lock:
            self.stats['total_operations'] += 1
            self.stats['total_gets'] += 1
            if key not in self.tuples:
                self.stats['total_errors'] += 1
                return (False, "ERR {} does not exist".format(key))
            else:
                value = self.tuples.pop(key)
                # Remove from stats
                self.stats['key_sizes'].remove(len(key))
                self.stats['value_sizes'].remove(len(value))
                return (True, "OK ({}, {}) removed".format(key, value))
    
    # Write the read instruction
    def read(self, key):
        with self.lock:
            self.stats['total_operations'] += 1
            self.stats['total_reads'] += 1
            if key not in self.tuples:
                self.stats['total_errors'] += 1
                return (False, "ERR {} does not exist".format(key))
            else:
                return (True, "OK ({}, {}) read".format(key, self.tuples[key]))
    
    # Server output
    def get_stats(self):
        with self.lock:
            num_tuples = len(self.tuples)
            avg_tuple_size = (sum(self.stats['key_sizes']) + sum(self.stats['value_sizes'])) / num_tuples if num_tuples > 0 else 0
            avg_key_size = sum(self.stats['key_sizes']) / num_tuples if num_tuples > 0 else 0
            avg_value_size = sum(self.stats['value_sizes']) / num_tuples if num_tuples > 0 else 0
            
            return {
                'num_tuples': num_tuples,
                'avg_tuple_size': avg_tuple_size,
                'avg_key_size': avg_key_size,
                'avg_value_size': avg_value_size,
                'total_clients': self.stats['total_clients'],
                'total_operations': self.stats['total_operations'],
                'total_reads': self.stats['total_reads'],
                'total_gets': self.stats['total_gets'],
                'total_puts': self.stats['total_puts'],
                'total_errors': self.stats['total_errors']
            }
    
    def increment_clients(self):
        with self.lock:
            self.stats['total_clients'] += 1

# Write Class ClientHandler, with each client connection corresponding to an independent thread.
class ClientHandler(threading.Thread):
    def __init__(self, conn, addr, tuple_space):
        threading.Thread.__init__(self)
        self.conn = conn
        self.addr = addr
        self.tuple_space = tuple_space
    
    def run(self):
        self.tuple_space.increment_clients()
        print("Client connected from:", self.addr)
        
        try:
            while True:
                # Receive message length first (3 digits)
                msg_length_str = self.conn.recv(3).decode('utf-8')
                if not msg_length_str:
                    break
                
                try:
                    msg_length = int(msg_length_str)
                except ValueError:
                    print("Invalid message length:", msg_length_str)
                    break
                
                # Receive the rest of the message
                remaining_bytes = msg_length - 3
                if remaining_bytes <= 0:
                    print("Invalid remaining bytes:", remaining_bytes)
                    break
                
                message = self.conn.recv(remaining_bytes).decode('utf-8')
                if not message:
                    break
                
                # Process the message
                operation = message[0]
                content = message[1:].strip()
                
                if operation == 'P':  # PUT
                    parts = content.split(' ', 1)
                    if len(parts) != 2:
                        response = "ERR invalid PUT format"
                    else:
                        key, value = parts
                        if len(key) + len(value) > 970:
                            response = "ERR key+value too long"
                        else:
                            success, response = self.tuple_space.put(key, value)
                
                elif operation == 'G':  # GET
                    key = content
                    success, response = self.tuple_space.get(key)
                
                elif operation == 'R':  # READ
                    key = content
                    success, response = self.tuple_space.read(key)
                
                else:
                    response = "ERR invalid operation"
                
                # Send response
                response_length = len(response)
                response_msg = f"{response_length:03d}{response}"
                self.conn.sendall(response_msg.encode('utf-8'))
        
        except ConnectionResetError:
            print("Client disconnected abruptly:", self.addr)
        finally:
            self.conn.close()
            print("Client disconnected:", self.addr)

# Displays the summary of the current tuple space
def print_stats(tuple_space, interval=10):
    while True:
        time.sleep(interval)
        stats = tuple_space.get_stats()
        print("Server Stats (every {}s):".format(interval))
        print("Tuples: {}".format(stats['num_tuples']))
        print("Avg tuple size: {:.2f}".format(stats['avg_tuple_size']))
        print("Avg key size: {:.2f}".format(stats['avg_key_size']))
        print("Avg value size: {:.2f}".format(stats['avg_value_size']))
        print("Total clients: {}".format(stats['total_clients']))
        print("Total operations: {}".format(stats['total_operations']))
        print("READs: {}, GETs: {}, PUTs: {}".format(
            stats['total_reads'], stats['total_gets'], stats['total_puts']))
        print("Errors: {}".format(stats['total_errors']))

def start_server(port):
    tuple_space = TupleSpace()
    
    # Start stats thread
    stats_thread = threading.Thread(target=print_stats, args=(tuple_space,), daemon=True)
    stats_thread.start()
    
    # Create server socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('0.0.0.0', port))
        s.listen()
        print("Server listening on port", port)
        
        try:
            while True:
                conn, addr = s.accept()
                client_handler = ClientHandler(conn, addr, tuple_space)
                client_handler.start()
        except KeyboardInterrupt:
            print("Server shutting down...")
        finally:
            s.close()
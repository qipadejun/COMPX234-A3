import socket
import sys
import time

class TupleSpaceClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def connect(self):
        try:
            self.socket.connect((self.host, self.port))
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False
    
    def send_request(self, operation, key, value=None):
        # Build request message
        if operation.upper() == 'P':  # PUT
            if value is None:
                raise ValueError("PUT operation requires a value")
            content = f"{operation.upper()} {key} {value}"
        elif operation.upper() in ('G', 'R'):  # GET or READ
            content = f"{operation.upper()} {key}"
        else:
            raise ValueError("Invalid operation")
        
        # Calculate message length (NNN)
        msg_length = len(content) + 3  # +3 for the length digits
        if msg_length > 999:
            raise ValueError("Message too long")
        
        # Format message with leading zeros for length
        message = f"{msg_length:03d}{content}"
        
        # Send message
        self.socket.sendall(message.encode('utf-8'))
        
        # Receive response
        # First get the length (3 digits)
        response_length_str = self.socket.recv(3).decode('utf-8')
        if not response_length_str:
            raise ConnectionError("Connection closed by server")
        
        try:
            response_length = int(response_length_str)
        except ValueError:
            raise ValueError("Invalid response length from server")
        
        # Get the rest of the response
        response = self.socket.recv(response_length).decode('utf-8')
        return response
    
    def close(self):
        self.socket.close()

def process_request_file(client, file_path):
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Parse the line
                parts = line.split()
                if len(parts) < 2:
                    print(f"Invalid line: {line}")
                    continue
                
                operation = parts[0]
                key = parts[1]
                value = ' '.join(parts[2:]) if len(parts) > 2 else None
                
                # Check collated size
                if value and (len(key) + len(value) + 1) > 970:  # +1 for space
                    print(f"{line}: ERR key+value too long")
                    continue
                
                try:
                    response = client.send_request(operation, key, value)
                    print(f"{line}: {response}")
                except Exception as e:
                    print(f"Error processing {line}: {e}")
                    break
                
                # Small delay to prevent overwhelming the server
                time.sleep(0.01)
    
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
    except Exception as e:
        print(f"Error processing file: {e}")
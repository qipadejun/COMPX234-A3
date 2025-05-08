import socket
import threading
import time
from collections import defaultdict

class TupleSpace:
    def __init__(self):
        self.tuples = {}
        self.lock = threading.Lock()
        self.stats = {
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
            

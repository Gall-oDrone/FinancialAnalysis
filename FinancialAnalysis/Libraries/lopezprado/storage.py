import redis
import threading
import time
from concurrent.futures import ThreadPoolExecutor

class RedisConnector:
    def __init__(self, host='localhost', port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db
        self.connection = None

    def connect(self):
        self.connection = redis.Redis(host=self.host, port=self.port, db=self.db)

    def get_connection(self):
        if not self.connection:
            self.connect()
        return self.connection

class RedisQueryServer:
    def __init__(self, redis_connector, query_interval=1):
        self.redis_connector = redis_connector
        self.query_interval = query_interval
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=5)

    def start(self, duration, time_unit='seconds'):
        self.running = True
        end_time = time.time() + duration if time_unit == 'seconds' else time.time() + duration * 60
        while time.time() < end_time:
            if not self.running:
                break
            self.executor.submit(self.query_data)
            time.sleep(self.query_interval)

    def stop(self):
        self.running = False
        self.executor.shutdown(wait=True)

    def query_data(self):
        connection = self.redis_connector.get_connection()
        # Replace 'your_key' with the key you want to query
        result = connection.get('your_key')
        print(f'Query result: {result}')
        
# Example usage:
if __name__ == "__main__":
    redis_connector = RedisConnector(host='localhost', port=6379, db=0)
    server = RedisQueryServer(redis_connector, query_interval=1)
    
    try:
        server.start(duration=10, time_unit='seconds')  # Run for 10 seconds
    finally:
        server.stop()

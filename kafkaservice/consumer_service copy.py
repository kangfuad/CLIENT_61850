import os
from confluent_kafka import Consumer, KafkaError
import signal
import sys
import json
import time
import logging
import socket
from typing import List, Optional
from urllib.parse import urlparse

class KafkaConsumerHandler:
    def __init__(self, broker: str, group_id: str, topics: List[str], 
                 max_retries: int = 5, retry_interval: int = 5):
        self.broker = broker
        self.group_id = group_id
        self.topics = topics if isinstance(topics, list) else [topics]
        self.running = True
        self.consumer = None
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.setup_logging()
        self.validate_broker()
        self.setup_consumer()
        self.setup_signals()

    def validate_broker(self):
        """Validasi broker address dan konektivitas"""
        try:
            # Parse broker URL
            broker_urls = self.broker.split(',')
            for broker_url in broker_urls:
                parsed = urlparse(f"//{broker_url}")
                host = parsed.hostname or broker_url.split(':')[0]
                port = parsed.port or 9092

                # Test koneksi TCP
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((host, port))
                sock.close()

                if result != 0:
                    self.logger.warning(f"Cannot connect to broker at {host}:{port}")
                    raise ConnectionError(f"Failed to connect to {host}:{port}")
                    
        except Exception as e:
            self.logger.error(f"Broker validation failed: {str(e)}")
            raise

    def setup_logging(self):
        """Setup logger dengan format yang lebih detail"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('kafka.consumer_service')

    def get_sasl_config(self) -> dict:
        """Get SASL configuration with validation"""
        required_env = ['SECURITY_PROTOCOL', 'SASL_MECHANISM', 'SASL_USERNAME', 'SASL_PASSWORD']
        
        config = {}
        missing_env = []
        
        for env_var in required_env:
            value = os.getenv(env_var)
            if not value:
                missing_env.append(env_var)
            else:
                config[env_var.lower()] = value
                
        if missing_env:
            self.logger.error(f"Missing required environment variables: {', '.join(missing_env)}")
            raise ValueError(f"Missing environment variables: {', '.join(missing_env)}")
            
        return config

    def setup_signals(self):
        """Setup signal handlers untuk graceful shutdown"""
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info("Shutdown signal received")
        self.running = False
        self.shutdown()

    def setup_consumer(self):
        """Inisialisasi consumer dengan retry logic"""
        retries = 0
        while retries < self.max_retries:
            try:
                sasl_config = self.get_sasl_config()
                
                config = {
                    'bootstrap.servers': self.broker,
                    'group.id': self.group_id,
                    'auto.offset.reset': 'earliest',
                    'enable.auto.commit': False,
                    'security.protocol': sasl_config['security_protocol'],
                    'sasl.mechanisms': sasl_config['sasl_mechanism'],
                    'sasl.username': sasl_config['sasl_username'],
                    'sasl.password': sasl_config['sasl_password'],
                    # Additional configurations for reliability
                    'session.timeout.ms': 45000,
                    'heartbeat.interval.ms': 15000,
                    'max.poll.interval.ms': 300000,
                    'reconnect.backoff.ms': 1000,
                    'reconnect.backoff.max.ms': 10000
                }

                self.consumer = Consumer(config)
                self.consumer.subscribe(self.topics)
                self.logger.info(f"Successfully subscribed to topics: {self.topics}")
                return
                
            except Exception as e:
                retries += 1
                self.logger.error(f"Attempt {retries}/{self.max_retries} failed: {str(e)}")
                if retries < self.max_retries:
                    time.sleep(self.retry_interval)
                else:
                    raise Exception(f"Failed to setup consumer after {self.max_retries} attempts")

    def process_message(self, message):
        """Process individual message with better error handling"""
        try:
            if not message or not message.value():
                self.logger.warning("Received empty message")
                return False

            value = message.value().decode('utf-8')
            
            try:
                data = json.loads(value)
                self.logger.debug(f"Successfully processed message from topic {message.topic()}")
                return True
            except json.JSONDecodeError as e:
                self.logger.warning(f"Invalid JSON message: {str(e)}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            return False

    def start_consuming(self):
        """Enhanced consume loop with better error recovery"""
        consecutive_errors = 0
        
        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue

                if msg.error():
                    if not self.handle_error(msg.error()):
                        consecutive_errors += 1
                        if consecutive_errors > self.max_retries:
                            self.logger.error("Too many consecutive errors, attempting restart")
                            self.restart_consumer()
                            consecutive_errors = 0
                        continue
                    
                if self.process_message(msg):
                    self.consumer.commit(msg)
                    consecutive_errors = 0
                
            except Exception as e:
                self.logger.error(f"Error in consume loop: {str(e)}")
                consecutive_errors += 1
                if consecutive_errors > self.max_retries:
                    self.restart_consumer()
                    consecutive_errors = 0
                time.sleep(self.retry_interval)

    def restart_consumer(self):
        """Restart consumer connection"""
        self.logger.info("Restarting consumer...")
        try:
            self.shutdown()
            time.sleep(self.retry_interval)
            self.setup_consumer()
        except Exception as e:
            self.logger.error(f"Failed to restart consumer: {str(e)}")
            self.running = False

# Example usage
if __name__ == "__main__":
    try:
        consumer = KafkaConsumerHandler(
            broker=os.getenv('BOOTSTRAP_SERVER', 'localhost:9092'),
            group_id="my-consumer-group",
            topics=["DATAPOINTS"],
            max_retries=5,
            retry_interval=5
        )
        consumer.start_consuming()
    except KeyboardInterrupt:
        print("Shutting down...")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)
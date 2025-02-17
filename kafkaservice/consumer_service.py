import os
import json
import time
import signal
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from sbo import IEDControlManager

class KafkaMessageConsumer:
    def __init__(self, bootstrap_servers, topic, group_id=None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id or f"consumer-group-{int(time.time())}"
        self.consumer = None
        self.is_running = True
        self.setup_logging()
        self.setup_signals()
        
    def setup_logging(self):
        """Setup logger configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def setup_signals(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)

    def handle_signal(self, signum, frame):
        """Handle termination signals"""
        self.logger.info("Received shutdown signal...")
        self.is_running = False
        if self.consumer:
            self.consumer.close()


    def connect(self):
        """Create and configure Kafka consumer"""
        try:
            self.logger.info(f"Connecting to Kafka at {self.bootstrap_servers}")
            
            # Basic authentication configuration if needed
            sasl_config = {}
            if os.getenv('SASL_USERNAME') and os.getenv('SASL_PASSWORD'):
                sasl_config.update({
                    'security_protocol': os.getenv('SECURITY_PROTOCOL', 'SASL_PLAINTEXT'),
                    'sasl_mechanism': os.getenv('SASL_MECHANISM', 'PLAIN'),
                    'sasl_plain_username': os.getenv('SASL_USERNAME'),
                    'sasl_plain_password': os.getenv('SASL_PASSWORD')
                })

            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',  # Start from earliest message
                enable_auto_commit=True,
                value_deserializer=lambda x: self.decode_message(x),
                **sasl_config
            )
            
            self.logger.info(f"Successfully connected to topic: {self.topic}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {str(e)}")
            return False

    def decode_message(self, message):
        """Decode and parse message value"""
        try:
            if message is None:
                return None
            decoded = message.decode('utf-8')
            return json.loads(decoded)
        except json.JSONDecodeError:
            self.logger.warning("Received message is not valid JSON")
            return decoded
        except Exception as e:
            self.logger.error(f"Error decoding message: {str(e)}")
            return None

    def process_message(self, message):
        """Process each message - override this method for custom processing 
        CSWI_IEDCSWI_Control/CSWI1.Pos.ctlModel
        """
        try:
            if (message.get('protocol') == 'IEC61880' and
            message.get('event') == 'COMMAND' and
            message.get('action') == 'SELECT_RTU'):
                dataComand = {
                    "id": message.get('tag'),
                    "value": message.get('value')
                }
            
                IEDControlManager.select(dataComand)
                # Log the message value if conditions are met
                self.logger.info(f"Value: {message.value}")
                self.logger.info("-" * 50)
                return True
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            return False

    def start_consuming(self):
        """Start consuming messages from Kafka"""
        if not self.connect():
            self.logger.error("Failed to start consumer due to connection error")
            return

        self.logger.info("Started consuming messages...")
        try:
            while self.is_running:
                try:
                    message_batch = self.consumer.poll(timeout_ms=1000)

                    if not message_batch:
                        continue

                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            self.process_message(message)

                except Exception as e:
                    self.logger.error(f"Error while consuming messages: {str(e)}")
                    time.sleep(1)  

        except KeyboardInterrupt:
            self.logger.info("Interrupted by user, shutting down...")
            self.shutdown()
        finally:
            self.shutdown()


    def shutdown(self):
        """Clean shutdown of consumer"""
        self.logger.info("Shutting down consumer...")
        if self.consumer:
            self.consumer.close()
        self.logger.info("Consumer has been shut down")

# if __name__ == "__main__":
#     # Example usage
#     consumer = KafkaMessageConsumer(
#         bootstrap_servers=os.getenv('BOOTSTRAP_SERVER', 'localhost:9092'),
#         topic='DATAPOINTS',
#         group_id='my-consumer-group'
#     )
    
#     try:
#         consumer.start_consuming()
#     except Exception as e:
#         logging.error(f"Fatal error: {str(e)}")
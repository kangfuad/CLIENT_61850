import signal
import logging
from typing import Optional
from .kafka_connection import KafkaConnectionManager

class KafkaMessageConsumer:
    def __init__(self, topic: str, group_id: str):
        
        self.topic = topic
        self.group_id = group_id
        self.is_running = True
        
        self.logger = logging.getLogger(__name__)
        self.connection_manager = KafkaConnectionManager()
        self.consumer = self.connection_manager.get_consumer(topic, group_id)
        
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        
    def _handle_signal(self, signum, frame):
        """Handle termination signals"""
        self.is_running = False
        self.shutdown()

    def process_message(self, message):
        """Process each message - override this method for custom processing"""
        try:
            # Add your custom processing logic here
            # Example:
            # if message.value.get('event') == 'COMMAND':
            #     return self._handle_command(message.value)
            return True
        except Exception as e:
            self.logger.error(f"Processing error: {str(e)}")
            return False

    def start_consuming(self):
        """Start consuming messages from Kafka"""
        if not self.consumer:
            self.logger.error("No Kafka connection available")
            return

        try:
            while self.is_running:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    continue

                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        self.process_message(message)

        except Exception as e:
            self.logger.error(f"Consumer error: {str(e)}")
        finally:
            self.shutdown()

    def shutdown(self):
        """Clean shutdown of consumer"""
        self.connection_manager.close_consumer(self.topic, self.group_id)

# if __name__ == "__main__":
#     # Example usage with connection manager
#     consumer = KafkaMessageConsumer(
#         topic='DATAPOINTS',
#         group_id='my-consumer-group'
#     )
    
#     try:
#         consumer.start_consuming()
#     except Exception as e:
#         logging.error(f"Fatal error: {str(e)}")
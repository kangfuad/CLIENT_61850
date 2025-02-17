import logging
from typing import Optional, Any
from .kafka_connection import KafkaConnectionManager


class KafkaProducerService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.connection_manager = KafkaConnectionManager()
        self.producer = self.connection_manager.get_producer()

    def send_message(self, topic: str, message: Any) -> bool:
        """Send message to specified Kafka topic"""
        if not self.producer:
            self.logger.error("No Kafka connection available")
            return False

        try:
            future = self.producer.send(topic, value=message)
            future.add_errback(lambda exc: 
                self.logger.error(f"Delivery failed: {str(exc)}"))
            
            self.producer.flush()
            return True

        except Exception as e:
            self.logger.error(f"Send error: {str(e)}")
            return False

    def close(self):
        """Close producer connection"""
        self.connection_manager.close_producer()
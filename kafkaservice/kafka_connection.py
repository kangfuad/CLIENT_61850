import os
import json
from kafka import KafkaProducer, KafkaConsumer
import logging
from typing import List, Optional, Dict, Any, Union

class KafkaConnectionManager:
    _instance = None
    _producer = None
    _consumers: Dict[str, KafkaConsumer] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaConnectionManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """Initialize logger and connection configs"""
        self.logger = logging.getLogger(__name__)
        self.bootstrap_servers = os.getenv('BOOTSTRAP_SERVER', 'localhost:9092')
        self._setup_sasl_config()
        
    def _setup_sasl_config(self) -> dict:
        """Setup SASL configuration if credentials are provided"""
        self.sasl_config = {}
        if os.getenv('SASL_USERNAME') and os.getenv('SASL_PASSWORD'):
            self.sasl_config.update({
                'security_protocol': os.getenv('SECURITY_PROTOCOL', 'SASL_PLAINTEXT'),
                'sasl_mechanism': os.getenv('SASL_MECHANISM', 'PLAIN'),
                'sasl_plain_username': os.getenv('SASL_USERNAME'),
                'sasl_plain_password': os.getenv('SASL_PASSWORD')
            })
        return self.sasl_config

    def get_producer(self) -> Optional[KafkaProducer]:
        """Get or create Kafka producer instance"""
        if self._producer is None:
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    **self.sasl_config
                )
                self.logger.info("Kafka producer connection established")
            except Exception as e:
                self.logger.error(f"Failed to create Kafka producer: {str(e)}")
                return None
        return self._producer

    def get_consumer(self, topics: Union[str, List[str]], group_id: str, **kwargs) -> Optional[KafkaConsumer]:
        """
        Get or create Kafka consumer instance for specific topics and group
        
        :param topics: Satu topik atau daftar topik
        :param group_id: Group ID konsumer
        """
        # Konversi topik tunggal menjadi list
        topics = [topics] if isinstance(topics, str) else topics
        
        # Buat kunci unik untuk konsumer
        consumer_key = f"{'-'.join(topics)}_{group_id}"
        
        if consumer_key not in self._consumers:
            try:
                consumer_config = {
                    'bootstrap_servers': self.bootstrap_servers,
                    'group_id': group_id,
                    'auto_offset_reset': 'earliest',
                    'enable_auto_commit': True,
                    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
                    **self.sasl_config,
                    **kwargs
                }
                
                self._consumers[consumer_key] = KafkaConsumer(
                    *topics,  # Gunakan unpacking untuk multi-topik
                    **consumer_config
                )
                self.logger.info(f"Kafka consumer connection established for topics: {topics}")
            except Exception as e:
                self.logger.error(f"Failed to create Kafka consumer for topics {topics}: {str(e)}")
                return None
                
        return self._consumers[consumer_key]



    def close_producer(self):
        """Close producer connection if exists"""
        if self._producer:
            try:
                self._producer.flush()
                self._producer.close()
                self._producer = None
                self.logger.info("Kafka producer connection closed")
            except Exception as e:
                self.logger.error(f"Error closing producer: {str(e)}")

    def close_consumer(self, topics: Union[str, List[str]], group_id: str):
        """
        Close specific consumer connection
        
        :param topics: Satu topik atau daftar topik
        :param group_id: Group ID konsumer
        """
        # Konversi topik tunggal menjadi list
        topics = [topics] if isinstance(topics, str) else topics
        
        # Buat kunci unik untuk konsumer
        consumer_key = f"{'-'.join(topics)}_{group_id}"
        
        if consumer_key in self._consumers:
            try:
                self._consumers[consumer_key].close()
                del self._consumers[consumer_key]
                self.logger.info(f"Kafka consumer connection closed for topics: {topics}")
            except Exception as e:
                self.logger.error(f"Error closing consumer for topics {topics}: {str(e)}")

    def close_all(self):
        """Close all Kafka connections"""
        self.close_producer()
        for consumer_key in list(self._consumers.keys()):
            topic = consumer_key.split('_')[0]
            group_id = consumer_key.split('_')[1]
            self.close_consumer(topic, group_id)
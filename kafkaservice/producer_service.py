from kafka import KafkaProducer
import json
import os
import logging

class KafkaProducerService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.producer = None
        self.connect()

    def connect(self):
        """Membuat koneksi ke Kafka broker"""
        try:
            # Konfigurasi autentikasi SASL jika diperlukan
            sasl_config = {}
            if os.getenv('SASL_USERNAME') and os.getenv('SASL_PASSWORD'):
                sasl_config.update({
                    'security_protocol': os.getenv('SECURITY_PROTOCOL', 'SASL_PLAINTEXT'),
                    'sasl_mechanism': os.getenv('SASL_MECHANISM', 'PLAIN'),
                    'sasl_plain_username': os.getenv('SASL_USERNAME'),
                    'sasl_plain_password': os.getenv('SASL_PASSWORD')
                })

            self.producer = KafkaProducer(
                bootstrap_servers=os.getenv('BOOTSTRAP_SERVER', 'localhost:9092'),
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                **sasl_config
            )
            self.logger.info("Successfully connected to Kafka broker")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka broker: {str(e)}")
            return False

    def on_send_success(self, record_metadata):
        """Callback untuk pengiriman pesan berhasil"""
        self.logger.info(f"Message delivered to {record_metadata.topic} [{record_metadata.partition}] at offset {record_metadata.offset}")

    def on_send_error(self, excp):
        """Callback untuk pengiriman pesan gagal"""
        self.logger.error(f"Message delivery failed: {str(excp)}")

    def send_message(self, topic, message):
        """Mengirim pesan ke topic Kafka yang ditentukan"""
        try:
            if not self.producer:
                if not self.connect():
                    raise Exception("Failed to connect to Kafka broker")

            # Kirim pesan secara asynchronous
            future = self.producer.send(topic, value=message)
            
            # Tambahkan callback
            # future.add_callback(self.on_send_success)
            future.add_errback(self.on_send_error)
            
            # Optional: tunggu hasil pengiriman
            # record_metadata = future.get(timeout=10)
            
            # Flush untuk memastikan pesan terkirim
            self.producer.flush()
            
            return True

        except Exception as e:
            self.logger.error(f"Error sending message: {str(e)}")
            return False

    def close(self):
        """Tutup koneksi producer"""
        try:
            if self.producer:
                self.producer.flush()  # Tunggu sampai semua pesan terkirim
                self.producer.close()  # Tutup koneksi
                self.logger.info("Producer closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing producer: {str(e)}")

# Contoh penggunaan:
# if __name__ == "__main__":
#     producer = KafkaProducerService()
#     producer.send_message("your-topic", {"key": "value"})
#     producer.close()
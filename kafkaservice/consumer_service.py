import signal
import logging
from typing import List, Optional, Union
from .kafka_connection import KafkaConnectionManager

class KafkaMessageConsumer:
    def __init__(self, topics: Union[str, List[str]], group_id: str):
        """
        Inisialisasi consumer dengan support multi-topik
        
        :param topics: Satu topik atau daftar topik
        :param group_id: Group ID untuk consumer
        """
        # Konversi topik tunggal menjadi list jika diperlukan
        self.topics = [topics] if isinstance(topics, str) else topics
        self.group_id = group_id
        self.is_running = True
        
        self.logger = logging.getLogger(__name__)
        self.connection_manager = KafkaConnectionManager()
        
        # Buat consumer dengan multi-topik
        self.consumer = self.connection_manager.get_consumer(
            topics=self.topics,  # Gunakan list topik
            group_id=group_id
        )
        
        # Tambahkan signal handlers
        self._setup_signal_handlers()

    def start_consuming(self):
        """Start consuming messages from multiple Kafka topics"""
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
                        # Tambahkan informasi topik ke pesan
                        processed_message = {
                            'topic': topic_partition.topic,
                            'value': message.value
                        }
                        self.process_message(processed_message)

        except Exception as e:
            self.logger.error(f"Consumer error: {str(e)}")
        finally:
            self.shutdown()

    def process_message(self, message):
        """
        Proses pesan dari berbagai topik
        
        :param message: Pesan dengan informasi topik
        """
        try:
            # Log pesan yang diterima untuk debugging
            self.logger.info(f"Menerima pesan dari topik {message.get('topic')}")
            return True

        except Exception as e:
            self.logger.error(f"Processing error untuk topik {message.get('topic')}: {str(e)}")
            return False

    def shutdown(self):
        """Clean shutdown of consumer"""
        try:
            self.logger.info("Memulai proses shutdown consumer...")
            self.is_running = False
            
            # Tutup consumer
            if self.consumer:
                self.connection_manager.close_consumer(self.topics, self.group_id)
            
            self.logger.info("Shutdown consumer berhasil")
        except Exception as e:
            self.logger.error(f"Error saat shutdown consumer: {e}")

    def _process_datapoints(self, message):
        """Proses pesan dari topik DATAPOINTS"""
        # Implementasi spesifik untuk DATAPOINTS
        self.logger.info(f"Memproses datapoint: {message}")
        return True

    def _process_ied_control(self, message):
        """Proses pesan dari topik IED_CONTROL"""
        # Implementasi spesifik untuk IED_CONTROL
        self.logger.info(f"Memproses kontrol IED: {message}")
        return True

    def _process_connection_state(self, message):
        """Proses pesan dari topik IED_CONNECTION_STATE"""
        # Implementasi spesifik untuk IED_CONNECTION_STATE
        self.logger.info(f"Memproses state koneksi: {message}")
        return True

    def _setup_signal_handlers(self):
        """
        Setup signal handlers untuk menangani interrupt
        """
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        """
        Handle termination signals
        
        :param signum: Nomor sinyal
        :param frame: Current stack frame
        """
        self.logger.info(f"Menerima sinyal {signum}. Memulai proses shutdown...")
        self.is_running = False
        self.shutdown()

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
from enum import Enum, auto
import json
import logging
import os
import concurrent.futures
import time
import threading
import queue
from typing import List, Dict
from dotenv import load_dotenv
from flask import Flask, jsonify
from libiec61850client import iec61850client
from kafkaservice.consumer_service import KafkaMessageConsumer
from kafkaservice.producer_service import KafkaProducerService
from database.db_connector import DatabaseConnector
from typing import Tuple, Optional, Dict

# Load variabel environment
load_dotenv()

# Ambil konfigurasi dari .env
GLOBAL_POLLING_ENABLED = os.getenv('GLOBAL_POLLING_ENABLED', 'true').lower() == 'true'
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', '5'))

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logging.getLogger('kafka').setLevel(logging.WARNING)

# Queue untuk menyimpan data yang diterima
data_queue = queue.Queue()

consumer = KafkaMessageConsumer(
    topics=['IED_CONTROL'],
    group_id='dev61850'
)

producer = KafkaProducerService()

class IEDConnection:
    def __init__(self, client, ied, datapoint_config):
        """
        Inisialisasi koneksi IED
        """
        self.client = client
        self.ied = ied
        self.datapoint_config = datapoint_config
        self.host = ied['ip']
        self.port = ied['port']
        self.references_to_register = self._get_references()

        self.current_state = IEDConnectionState.DISCONNECTED

        # Konfigurasi polling individual
        self.polling_config = ied.get('polling', {
            'enabled': True,
            'interval': 5000,
            'mode': 'periodic'
        })
        
        # Validasi konfigurasi polling
        self.polling_enabled = self.polling_config.get('enabled', True)
        self.polling_interval = max(1000, self.polling_config.get('interval', 5000))
        self.polling_mode = self.polling_config.get('mode', 'periodic')
        
    def _get_references(self):
        """
        Dapatkan referensi datapoint yang diaktifkan
        """
        # Cari datapoint untuk IED spesifik ini
        ied_datapoints = next(
            (sim['datapoints'] for sim in self.datapoint_config.get('simulators', []) 
            if sim['id'] == self.ied.get('id')), 
            []
        )
        
        # Gunakan datapoint yang diaktifkan
        return [
            f"iec61850://{self.host}:{self.port}/{dp['reference']}" 
            for dp in ied_datapoints if dp.get('enabled', False)
        ]

    def update_state(self, new_state):
        """
        Update state koneksi dengan thread-safe
        """
        if self.current_state != new_state:
            self.current_state = new_state
            # Tambahkan logging atau notifikasi jika diperlukan
            logger.info(f"IED {self.host}:{self.port} state berubah menjadi {new_state.name}")
    
    def check_and_reconnect(self):
        """
        Metode untuk mencoba reconnect secara otomatis
        """
        if (self.current_state == IEDConnectionState.ERROR or 
            self.current_state == IEDConnectionState.DISCONNECTED):
            
            logger.info(f"Mencoba reconnect ke IED {self.host}:{self.port}")
            return self.connect_and_register()
        
        return False

    def connect_and_register(self):
        """
        Lakukan koneksi dan registrasi datapoint
        """
        try:
            # Dapatkan model data
            model = self.client.getDatamodel(hostname=self.host, port=self.port)
            
            if not model:
                logger.warning(f"Tidak dapat mendapatkan model dari {self.host}:{self.port}")
                return False

            logger.info(f"Berhasil mendapatkan model dari {self.host}:{self.port}")
            
            # Ekstrak dan mapping data model
            extract_and_map_model_data(
                self.client, 
                model, 
                self.host, 
                self.port
            )
            
            # Registrasi referensi untuk pembacaan SEKALI
            for ref in self.references_to_register:
                try:
                    result = self.client.registerReadValue(ref)
                    if result == 0:
                        logger.info(f"Berhasil registrasi: {ref}")
                    else:
                        logger.warning(f"Gagal registrasi: {ref}")
                except Exception as reg_error:
                    logger.error(f"Error saat registrasi {ref}: {reg_error}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error saat koneksi dan registrasi IED {self.host}:{self.port}: {e}")
            return False

class IEDControlManager:
    def __init__(self, client: iec61850client, logger: Optional[logging.Logger] = None):
        """
        Inisialisasi manager kontrol untuk IED
        
        :param client: Klien IEC 61850
        :param logger: Logger kustom (opsional)
        """
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    def select(self, ref: str, value: str) -> Tuple[bool, Optional[str]]:
        """
        Melakukan operasi select pada objek kontrol

        :param ref: Referensi lengkap objek kontrol (iec61850://host:port/referensi)
        :param value: Nilai untuk select
        :return: Tuple (status keberhasilan, pesan tambahan)
        """
        try:
            # Panggil method select dari client
            error, add_cause = self.client.select(ref, value)
            
            if error == 1:
                self.logger.info(f"Berhasil select {ref} dengan nilai {value}")
                return True, None
            else:
                self.logger.error(f"Gagal select {ref}. Penyebab: {add_cause}")
                return False, add_cause
        
        except Exception as e:
            self.logger.error(f"Error saat select {ref}: {e}")
            return False, str(e)

    def operate(self, ref: str, value: str) -> Tuple[bool, Optional[str]]:
        """
        Melakukan operasi operate pada objek kontrol

        :param ref: Referensi lengkap objek kontrol (iec61850://host:port/referensi)
        :param value: Nilai untuk operate
        :return: Tuple (status keberhasilan, pesan tambahan)
        """
        try:
            # Panggil method operate dari client
            error, add_cause = self.client.operate(ref, value)
            
            if error == 1:
                self.logger.info(f"Berhasil operate {ref} dengan nilai {value}")
                return True, None
            else:
                self.logger.error(f"Gagal operate {ref}. Penyebab: {add_cause}")
                return False, add_cause
        
        except Exception as e:
            self.logger.error(f"Error saat operate {ref}: {e}")
            return False, str(e)

    def cancel(self, ref: str) -> Tuple[bool, Optional[str]]:
        """
        Melakukan operasi cancel pada objek kontrol

        :param ref: Referensi lengkap objek kontrol (iec61850://host:port/referensi)
        :return: Tuple (status keberhasilan, pesan tambahan)
        """
        try:
            # Panggil method cancel dari client
            error = self.client.cancel(ref)
            
            if error == 1:
                self.logger.info(f"Berhasil cancel {ref}")
                return True, None
            else:
                self.logger.error(f"Gagal cancel {ref}")
                return False, "Operasi cancel gagal"
        
        except Exception as e:
            self.logger.error(f"Error saat cancel {ref}: {e}")
            return False, str(e)
        
class IEDConnectionState(Enum):
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    ERROR = auto()

def run_consumer():
    consumer.start_consuming()

def load_ied_config(config_path: str = 'ied_config.json') -> List[Dict]:
    """
    Load konfigurasi IED dari file JSON
    
    :param config_path: Path ke file konfigurasi JSON
    :return: Daftar konfigurasi IED yang diaktifkan
    """
    try:
        # Cetak path absolut untuk debugging
        abs_path = os.path.abspath(config_path)
        logger.info(f"Mencoba membaca konfigurasi dari: {abs_path}")

        # Tambahkan penanganan jika file tidak ada
        if not os.path.exists(abs_path):
            logger.error(f"File konfigurasi tidak ditemukan: {abs_path}")
            return []

        with open(abs_path, 'r') as f:
            config = json.load(f)
        
        # Filter hanya IED yang diaktifkan 
        # Gunakan 'enabled' dan pastikan nilainya True
        enabled_ieds = [ied for ied in config if ied.get('enabled', False)]
        
        # Log informasi untuk debugging
        logger.info(f"Total IED dalam konfigurasi: {len(config)}")
        logger.info(f"IED yang diaktifkan: {len(enabled_ieds)}")
        
        return enabled_ieds
    except FileNotFoundError:
        logger.error(f"Konfigurasi file tidak ditemukan: {config_path}")
        return []
    except json.JSONDecodeError:
        logger.error(f"Error parsing JSON dari file: {config_path}")
        return []
    except Exception as e:
        logger.error(f"Error membaca konfigurasi: {e}")
        return []

def load_datapoint_config(config_path: str = 'datapoint_register.json') -> Dict:
    """
    Load konfigurasi datapoint dari file JSON
    """
    try:
        # Cetak path absolut untuk debugging
        abs_path = os.path.abspath(config_path)
        logger.info(f"Mencoba membaca datapoint dari: {abs_path}")

        # Tambahkan penanganan jika file tidak ada
        if not os.path.exists(abs_path):
            logger.error(f"File datapoint tidak ditemukan: {abs_path}")
            return {}

        with open(abs_path, 'r') as f:
            config = json.load(f)
        
        return config
    except FileNotFoundError:
        logger.error(f"Konfigurasi datapoint tidak ditemukan: {config_path}")
        return {}
    except json.JSONDecodeError:
        logger.error(f"Error parsing JSON dari file: {config_path}")
        return {}
    except Exception as e:
        logger.error(f"Error membaca datapoint: {e}")
        return {}

def read_value_callback(ref, value):
    """
    Callback untuk membaca nilai yang diperbarui
    Pastikan struktur data konsisten
    """
    # Pastikan value adalah dictionary dengan struktur yang benar
    if not isinstance(value, dict):
        value = {
            'reftype': 'DA',
            'FC': '',
            'value': str(value),
            'type': type(value).__name__
        }
    
    # Tambahkan ke queue dengan struktur yang konsisten
    data_queue.put({
        'type': 'read_value',
        'ref': ref,
        'value': value
    })

# Untuk report_callback, pastikan struktur serupa
def report_callback(key, value):
    """
    Callback untuk laporan RCB
    Pastikan struktur data konsisten
    """
    # Pastikan value adalah dictionary dengan struktur yang benar
    if not isinstance(value, dict):
        value = {
            'reftype': 'DA',
            'FC': '',
            'value': str(value),
            'type': type(value).__name__
        }
    
    # Tambahkan ke queue dengan struktur yang konsisten
    data_queue.put({
        'type': 'report',
        'key': key,
        'value': value
    })

def command_termination_callback(message):
    """
    Callback untuk terminasi perintah
    """
    logger.info(f"Command Termination: {message}")

def data_processor():
    """
    Thread untuk memproses data dari queue
    """
    while True:
        try:
            # Tunggu data dengan timeout
            data = data_queue.get(timeout=10)
            
            # Proses data sesuai tipe
            if data['type'] == 'read_value':
                # Jalankan producer (contoh mengirim pesan)
                producer.send_message("61850_EVENTS", data)
                # logger.info(f"Processing read value: {data}")
            
            elif data['type'] == 'report':
                # logger.info(f"Processing report: {data}")
                # Jalankan producer (contoh mengirim pesan)
                producer.send_message("61850_EVENTS", data)
                # producer.flush()
            
            # Tandai tugas selesai
            data_queue.task_done()
        
        except queue.Empty:
            # Tidak ada data, lanjutkan
            continue
        except Exception as e:
            logger.error(f"Error dalam memproses data: {e}")

def extract_and_map_model_data(client, model, host, port):
    """
    Ekstrak dan mapping data dari model IED secara efisien
    
    :param client: Klien IEC 61850
    :param model: Model data dari IED
    :param host: Alamat host IED
    :param port: Port IED
    """
    # Proses mapping dilakukan hanya jika polling global aktif
    if not GLOBAL_POLLING_ENABLED:
        return

    # Queue untuk menampung proses mapping
    mapping_queue = queue.Queue()

    def recursive_mapping(current_model, base_ref=''):
        """
        Fungsi rekursif untuk mapping data dari model
        """
        for key, value in current_model.items():
            full_ref = f"{base_ref}/{key}" if base_ref else key

            if isinstance(value, dict):
                # Pastikan memiliki struktur yang konsisten
                if 'value' in value and 'type' in value:
                    try:
                        complete_ref = f"iec61850://{host}:{port}/{full_ref}"
                        
                        # Seragamkan struktur data
                        read_value_callback(complete_ref, {
                            'reftype': value.get('reftype', 'DA'),
                            'FC': value.get('FC', ''),
                            'value': str(value['value']),  # Konversi ke string
                            'type': value['type']
                        })
                    except Exception as e:
                        logger.error(f"Error mapping {full_ref}: {e}")
                
                # Lanjutkan rekursi
                recursive_mapping(value, full_ref)

    # Mulai proses mapping
    start_time = time.time()
    recursive_mapping(model)

    # Proses data dari queue secara parallel
    def process_mapping_queue():
        while not mapping_queue.empty():
            try:
                # Ambil data dari queue
                data = mapping_queue.get(timeout=1)
                
                # Lempar ke read_value_callback
                read_value_callback(data['ref'], data['value'])
                
                # Tandai tugas selesai
                mapping_queue.task_done()
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error memproses mapping: {e}")

    # Gunakan thread pool untuk pemrosesan parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, os.cpu_count() * 2)) as executor:
        # Jalankan proses mapping dalam beberapa thread
        futures = [
            executor.submit(process_mapping_queue) 
            for _ in range(min(5, os.cpu_count()))
        ]
        
        # Tunggu semua futures selesai
        concurrent.futures.wait(futures)

    # Log waktu eksekusi
    end_time = time.time()
    logger.debug(f"Model mapping selesai dalam {end_time - start_time:.4f} detik")

def ied_polling_thread(ied_connection):
    """
    Thread untuk polling data dari satu IED dengan konfigurasi flexible
    """
    while True:
        try:
            # Tentukan apakah polling diizinkan
            is_polling_allowed = (
                GLOBAL_POLLING_ENABLED and 
                ied_connection.polling_config.get('enabled', True)
            )

            if is_polling_allowed:
                # Log detail polling
                logger.debug(
                    f"Polling aktif untuk IED {ied_connection.host}:{ied_connection.port} "
                    f"- Interval: {ied_connection.polling_config.get('interval', POLLING_INTERVAL)}ms "
                    f"- Status: {ied_connection.current_state.name}"
                )
                
                # Periksa status koneksi
                if ied_connection.current_state != IEDConnectionState.CONNECTED:
                    logger.warning(
                        f"IED {ied_connection.host}:{ied_connection.port} "
                        f"tidak terkoneksi. Status: {ied_connection.current_state.name}. "
                        "Mencoba reconnect..."
                    )
                    
                    # Coba reconnect
                    ied_connection.check_and_reconnect()
                    time.sleep(10)  # Tunggu sebelum mencoba lagi
                    continue

                # Dapatkan model data terbaru
                try:
                    model = ied_connection.client.getDatamodel(
                        hostname=ied_connection.host, 
                        port=ied_connection.port
                    )
                    
                    if model:
                        # Ekstrak dan mapping data model
                        extract_and_map_model_data(
                            ied_connection.client, 
                            model, 
                            ied_connection.host, 
                            ied_connection.port
                        )
                    else:
                        logger.warning(
                            f"Gagal mendapatkan model untuk "
                            f"{ied_connection.host}:{ied_connection.port}"
                        )
                    
                    # Polling data
                    ied_connection.client.poll()
                
                except Exception as model_error:
                    # Update state ke ERROR jika gagal
                    ied_connection.update_state(IEDConnectionState.ERROR)
                    logger.error(
                        f"Error saat mendapatkan model/polling IED "
                        f"{ied_connection.host}:{ied_connection.port}: {model_error}"
                    )
            else:
                logger.debug(
                    f"Polling non-aktif untuk IED {ied_connection.host}:{ied_connection.port}. "
                    "Hanya mengandalkan RCB."
                )
            
            # Gunakan interval spesifik IED atau global
            polling_interval = ied_connection.polling_config.get('interval', POLLING_INTERVAL)
            
            # Pastikan interval minimal 1 detik
            polling_interval = max(1000, polling_interval)
            
            # Tunggu sesuai interval
            time.sleep(polling_interval / 1000)
        
        except Exception as e:
            logger.error(
                f"Error tidak terduga saat polling IED "
                f"{ied_connection.host}:{ied_connection.port}: {e}"
            )
            
            # Tunggu sebentar sebelum mencoba lagi
            time.sleep(10)

# Tambahkan fungsi untuk me-reload .env
def reload_polling_config():
    """
    Reload konfigurasi polling dari .env
    """
    global GLOBAL_POLLING_ENABLED, POLLING_INTERVAL
    load_dotenv(reload=True)
    GLOBAL_POLLING_ENABLED = os.getenv('GLOBAL_POLLING_ENABLED', 'true').lower() == 'true'
    POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', '5'))
    logger.info(f"Konfigurasi polling diperbarui - Aktif: {GLOBAL_POLLING_ENABLED}, Interval: {POLLING_INTERVAL}")

# Thread untuk monitoring perubahan .env
def env_monitor_thread():
    """
    Thread untuk memantau perubahan .env
    """
    last_modified = os.path.getmtime('.env')
    while True:
        try:
            current_modified = os.path.getmtime('.env')
            if current_modified != last_modified:
                logger.info("Terdeteksi perubahan file .env")
                reload_polling_config()
                last_modified = current_modified
            
            time.sleep(5)  # Periksa setiap 5 detik
        
        except Exception as e:
            logger.error(f"Error di env monitor thread: {e}")

def main():
    # Gunakan stop_event yang dibuat di program utama
    global stop_event  # Tambahkan ini untuk mengakses stop_event

    # Inisiasi client IEC 61850
    client = iec61850client(
        readvaluecallback=read_value_callback,
        cmdTerm_cb=command_termination_callback,
        Rpt_cb=report_callback
    )

    # Load konfigurasi
    ied_configs = load_ied_config()
    datapoint_config = load_datapoint_config()
    
    if not ied_configs:
        logger.error("Tidak ada IED yang diaktifkan dalam konfigurasi")
        return

    # Modifikasi fungsi polling untuk cek stop_event
    def ied_polling_thread(ied_connection):
        while not stop_event.is_set():  # Cek apakah sudah diberi sinyal stop
            try:
                # Periksa apakah polling diaktifkan untuk IED ini
                if not ied_connection.polling_enabled:
                    logger.info(f"Polling dinonaktifkan untuk IED {ied_connection.host}:{ied_connection.port}")
                    stop_event.wait(60)  # Tunggu 1 menit sebelum memeriksa ulang
                    continue

                # Periksa state sebelum polling
                if ied_connection.current_state != IEDConnectionState.CONNECTED:
                    # Coba reconnect jika tidak terkoneksi
                    ied_connection.check_and_reconnect()
                    stop_event.wait(10)  # Tunggu sebelum mencoba lagi
                    continue

                # Polling sesuai konfigurasi IED
                logger.debug(f"Polling IED {ied_connection.host}:{ied_connection.port} - Interval: {ied_connection.polling_interval}ms")
                
                # Dapatkan model data
                model = ied_connection.client.getDatamodel(
                    hostname=ied_connection.host, 
                    port=ied_connection.port
                )
                
                if model:
                    extract_and_map_model_data(
                        ied_connection.client, 
                        model, 
                        ied_connection.host, 
                        ied_connection.port
                    )
                
                # Polling data
                ied_connection.client.poll()
                
                # Gunakan interval spesifik IED
                stop_event.wait(ied_connection.polling_interval / 1000)
                
            except Exception as e:
                logger.error(f"Error saat polling IED {ied_connection.host}:{ied_connection.port}: {e}")
                stop_event.wait(10)  # Tunggu 10 detik sebelum mencoba lagi

    # Thread untuk memproses data
    data_processor_thread = threading.Thread(target=data_processor, daemon=True)
    data_processor_thread.start()

    # Daftar thread polling
    polling_threads = []
    ied_connections = []

    try:
        # Buat koneksi dan thread polling untuk setiap IED
        for ied in ied_configs:
            polling_config = ied.get('polling', {})
            logger.info(f"Konfigurasi Polling untuk IED {ied['id']}:")
            logger.info(f"  Enabled: {polling_config.get('enabled', True)}")
            logger.info(f"  Interval: {polling_config.get('interval', 5000)} ms")
            logger.info(f"  Mode: {polling_config.get('mode', 'periodic')}")
            ied_connection = IEDConnection(client, ied, datapoint_config)
            
            if ied_connection.connect_and_register():
                ied_connections.append(ied_connection)
                
                polling_thread = threading.Thread(
                    target=ied_polling_thread, 
                    args=(ied_connection,),
                    daemon=True
                )
                polling_thread.start()
                polling_threads.append(polling_thread)
            else:
                logger.error(f"Gagal membuat koneksi untuk IED {ied['ip']}:{ied['port']}")

        # Tunggu sampai stop_event di-set
        while not stop_event.is_set():
            stop_event.wait(1)  # Cek setiap 1 detik

    except Exception as e:
        logger.error(f"Error di main program: {e}")
    finally:
        logger.info("Membersihkan resources...")

if __name__ == "__main__":
    # 1. Buat event untuk mengontrol kapan program harus berhenti
    stop_event = threading.Event()
    # 2. List untuk menyimpan semua thread yang berjalan
    threads = []
    
    # 3. Buat fungsi khusus untuk menjalankan consumer
    def run_consumer():
        try:
            print("Consumer mulai berjalan...")
            consumer.start_consuming()
        except Exception as e:
            print(f"Terjadi error di consumer: {e}")
        
    # 4. Buat fungsi khusus untuk menjalankan program utama
    def run_main():
        try:
            print("Program utama mulai berjalan...")
            main()
        except Exception as e:
            print(f"Terjadi error di program utama: {e}")

    try:
        print("Memulai program...")
        
        # 5. Jalankan consumer di thread terpisah
        print("Menjalankan consumer di background...")
        consumer_thread = threading.Thread(
            name="ConsumerThread",  # Beri nama thread agar mudah dikenali
            target=run_consumer, 
            daemon=True  # Thread akan otomatis berhenti jika program utama berhenti
        )
        consumer_thread.start()
        threads.append(consumer_thread)
        
        # 6. Jalankan main program di thread terpisah
        print("Menjalankan program utama di background...")
        main_thread = threading.Thread(
            name="MainThread", 
            target=run_main,
            daemon=True
        )
        main_thread.start()
        threads.append(main_thread)

        # 7. Sekarang kita bisa jalankan kode lain (misal database) karena consumer sudah tidak blocking
        # print("Menjalankan operasi database...")
        db = DatabaseConnector()
        db.connect()

        # # Contoh query database
        # select_query = "SELECT * FROM app limit 1;"
        # messages = db.execute_query(select_query, return_result=True)
        # if messages:
        #     print("📝 Data dari database:")
        #     for msg in messages:
        #         print(f"ID: {msg[0]}, Content: {msg[1]}, Timestamp: {msg[2]}")

        # 8. Buat program tetap berjalan sampai ada CTRL+C
        print("Program berjalan... (Tekan CTRL+C untuk berhenti)")
        while True:
            # Cek setiap 1 detik apakah ada thread yang error
            time.sleep(1)
            
            # Cek apakah semua thread masih hidup
            if not all(t.is_alive() for t in threads):
                print("Salah satu thread berhenti secara tidak normal!")
                break
            
    except KeyboardInterrupt:
        # 9. Ketika CTRL+C ditekan
        print("\nMenerima sinyal CTRL+C, mulai proses shutdown...")
        
        # Matikan consumer dengan rapi
        print("Menghentikan consumer...")
        consumer.shutdown()
        
        # Beri tanda ke semua thread untuk berhenti
        print("Memberi sinyal ke semua thread untuk berhenti...")
        stop_event.set()
        
        # Tunggu semua thread selesai (maksimal 5 detik per thread)
        print("Menunggu semua thread berhenti...")
        for t in threads:
            print(f"Menunggu thread '{t.name}' berhenti...")
            t.join(timeout=5)

        # Tutup koneksi database
        db.disconnect()

        # Jangan lupa tutup producer di akhir
        producer.close()
            
        print("Semua thread sudah berhenti")
        print("Program selesai!")
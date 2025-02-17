import psycopg2
import os
from dotenv import load_dotenv
from psycopg2 import OperationalError, DatabaseError

# Memuat variabel lingkungan dari file .env
load_dotenv()

class DatabaseConnector:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT')),  # Pastikan port adalah integer
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        self.connection = None

    def connect(self):
        """Membuat koneksi ke database PostgreSQL."""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            print("✅ Connected to PostgreSQL database.")
        except OperationalError as e:
            print(f"❌ Failed to connect to database: {e}")
            self.connection = None

    def disconnect(self):
        """Menutup koneksi ke database."""
        if self.connection:
            self.connection.close()
            print("🔌 Disconnected from PostgreSQL database.")

    def execute_query(self, query, params=None, return_result=False):
        """
        Menjalankan query ke database.
        - return_result=True: Untuk SELECT (mengembalikan hasil query).
        - return_result=False: Untuk INSERT/UPDATE/DELETE (mengembalikan rowcount).
        """
        if not self.connection:
            print("⚠️ No active database connection.")
            return None

        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()

            if return_result:
                # Untuk SELECT
                return cursor.fetchall()
            else:
                # Untuk INSERT/UPDATE/DELETE
                return cursor.rowcount

        except DatabaseError as e:
            print(f"❌ Error executing query: {e}")
            self.connection.rollback()
            return None
        finally:
            if cursor:
                cursor.close()

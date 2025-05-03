import os
from contextlib import contextmanager
from dotenv import load_dotenv
import psycopg2

load_dotenv('/opt/airflow/.env')

class PgConnect:
    def __init__(self, host=None, port=None, db_name=None,
                 user=None, pw=None,
                 sslmode='require', sslrootcert=None):
        self.host = host or os.getenv('PG_HOST')
        self.port = int(port or os.getenv('PG_PORT', 5432))
        self.db_name = db_name or os.getenv('PG_DB')
        self.user = user or os.getenv('PG_USER')
        self.pw = pw or os.getenv('PG_PASSWORD')
        self.sslmode = sslmode or os.getenv('PG_SSLMODE', 'require')
        self.sslrootcert = sslrootcert or os.getenv('PG_SSLROOTCERT')

    def url(self):
        parts = [
            f"host={self.host}",
            f"port={self.port}",
            f"dbname={self.db_name}",
            f"user={self.user}",
            f"password={self.pw}",
            f"sslmode={self.sslmode}",
        ]
        if self.sslrootcert:
            parts.append(f"sslrootcert={self.sslrootcert}")
        return ' '.join(parts)

    def client(self):
        return psycopg2.connect(self.url())

    @contextmanager
    def connection(self):
        conn = psycopg2.connect(self.url())
        try:
            yield conn
            conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            conn.close()

    @classmethod
    def from_env(cls):
        return cls()

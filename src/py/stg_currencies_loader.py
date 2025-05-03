import csv
import io
import logging
from vertica_python import connect
from lib.pg_connect import PgConnect


class StgCurrenciesLoader:
    def __init__(self, pg_src: PgConnect, vert_cfg: dict, logger: logging.Logger):
        self.pg_src = pg_src
        self.vert_cfg = vert_cfg
        self.log = logger

    def load_currencies(self):
        with self.pg_src.connection() as pg_conn:
            with pg_conn.cursor(name="pg_stream_cursor") as cur:
                self.log.info("Extracting currencies from Postgres …")
                cur.execute(
                    """
                    SELECT  
                        date_update,
                        currency_code,
                        currency_code_with,
                        currency_with_div
                    FROM public.currencies
                    """
                )
                rows = cur.fetchall()
                self.log.info("Fetched %d currency rows", len(rows))

        if not rows:
            self.log.info("Nothing to load — quit.")
            return

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerows(rows)
        buf.seek(0)

        with connect(**self.vert_cfg) as vconn:
            vcur = vconn.cursor()
            self.log.info("Truncating staging.currencies …")
            vcur.execute('TRUNCATE TABLE "STV2025011438__STAGING".currencies')

            self.log.info("COPYing rows into staging.currencies …")
            vcur.copy(
                '''
                COPY "STV2025011438__STAGING".currencies (
                    date_update,
                    currency_code,
                    currency_code_with,
                    currency_with_div
                )
                FROM STDIN DELIMITER ',' NULL '' ENCLOSED BY '"'
                ''',
                buf
            )
            vconn.commit()

        self.log.info("Currencies loaded to staging via COPY (%d rows).", len(rows))

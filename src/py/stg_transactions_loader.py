import io
import logging
from datetime import date, timedelta

from lib.pg_connect import PgConnect
from vertica_python import connect


class StgTransactionsLoader:
    def __init__(self, pg_src: PgConnect, vert_cfg: dict, logger: logging.Logger):
        self.pg_src = pg_src
        self.vert_cfg = vert_cfg
        self.log = logger

    def load_transactions(self, execution_date: date):
        next_date = execution_date + timedelta(days=1)

        with self.pg_src.connection() as pg_conn:
            with pg_conn.cursor(name="pg_stream_cursor") as cur:
                self.log.info("Extracting transactions for %s from Postgres as stream…", execution_date)

                cur.itersize = 10000
                cur.execute("""
                    SELECT
                        operation_id,
                        account_number_from,
                        account_number_to,
                        currency_code,
                        country,
                        status,
                        transaction_type,
                        amount,
                        transaction_dt
                    FROM public.transactions
                    WHERE transaction_dt::DATE = %s;
                """, (execution_date,))

                output = io.StringIO()
                write = output.write

                row_count = 0
                for row in cur:
                    values = [str(v) if v is not None else '' for v in row]
                    write(','.join(values) + '\n')
                    row_count += 1
                    if row_count % 100000 == 0:
                        self.log.info("Processed %d rows…", row_count)

                if row_count == 0:
                    self.log.warning("No rows found for %s — skipping load.", execution_date)
                    return

                output.seek(0)

        with connect(**self.vert_cfg) as vconn:
            vcur = vconn.cursor()

            self.log.info("Deleting existing rows from staging.transactions for %s…", execution_date)
            vcur.execute("""
                DELETE FROM "STV2025011438__STAGING".transactions
                WHERE transaction_dt::DATE = %s
            """, (execution_date,))

            self.log.info("COPYing into staging.transactions …")
            vcur.copy(
                '''
                COPY "STV2025011438__STAGING".transactions (
                    operation_id,
                    account_number_from,
                    account_number_to,
                    currency_code,
                    country,
                    status,
                    transaction_type,
                    amount,
                    transaction_dt
                )
                FROM STDIN DELIMITER ',' NULL '' ENCLOSED BY '"'
                ''',
                output
            )

            vconn.commit()
            self.log.info("Successfully loaded %d rows into Vertica for %s", row_count, execution_date)

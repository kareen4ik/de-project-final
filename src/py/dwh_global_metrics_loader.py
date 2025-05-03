import logging
from vertica_python import connect


class DWHGlobalMetricsLoader:
    def __init__(self, vert_cfg: dict, logger: logging.Logger):
        self.cfg = vert_cfg
        self.log = logger

    def load_global_metrics(self, execution_date: str):
        date = execution_date

        with connect(**self.cfg) as conn:
            cur = conn.cursor()

            delete_sql = f"""
DELETE FROM "STV2025011438__DWH".global_metrics
 WHERE date_update = '{date}';
"""
            self.log.info("Deleting existing metrics for %s", date)
            cur.execute(delete_sql)

            insert_sql = f"""
INSERT INTO "STV2025011438__DWH".global_metrics (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
SELECT
    ft.date_update,
    ft.currency_code AS currency_from,
    SUM((ft.amount / 100.0) * c.currency_with_div) AS amount_total,
    COUNT(*) AS cnt_transactions,
    ROUND(
        COUNT(*)::DECIMAL /
        NULLIF(COUNT(DISTINCT ft.account_number_from), 0), 2
    ) AS avg_transactions_per_account,
    COUNT(DISTINCT ft.account_number_from) AS cnt_accounts_make_transactions
FROM (
    SELECT
        t.operation_id,
        t.account_number_from,
        t.status,
        t.amount,
        t.transaction_dt::DATE AS date_update,
        t.currency_code,
        ROW_NUMBER() OVER (
            PARTITION BY t.operation_id
            ORDER BY t.transaction_dt DESC
        ) AS rn
    FROM "STV2025011438__STAGING".transactions t
    WHERE t.transaction_dt::DATE = '{date}'
      AND t.status = 'done'
      AND t.account_number_from >= 0
) AS ft
JOIN "STV2025011438__STAGING".currencies c
  ON c.currency_code = ft.currency_code
 AND c.currency_code_with = 420
 AND c.date_update::DATE = ft.date_update::DATE
WHERE ft.rn = 1
GROUP BY ft.date_update, ft.currency_code;
"""
            self.log.info("Inserting computed metrics for %s", date)
            cur.execute(insert_sql)

            self.log.warning("Inserted %d rows for date %s", cur.rowcount, date)

            conn.commit()
            self.log.info("Load_global_metrics finished for %s", date)

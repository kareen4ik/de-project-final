CREATE TABLE "STV2025011438__STAGING".transactions (
    operation_id           VARCHAR(36)     NOT NULL, 
    account_number_from    BIGINT          NOT NULL,
    account_number_to      BIGINT          NOT NULL,
    currency_code          INT             NOT NULL,  
    country                VARCHAR(50)     NOT NULL,
    status                 VARCHAR(20)     NOT NULL,
    transaction_type       VARCHAR(30)     NOT NULL,
    amount                 BIGINT          NOT NULL,  
    transaction_dt         TIMESTAMP(3)    NOT NULL  
);

CREATE TABLE "STV2025011438__STAGING".currencies (
    date_update         TIMESTAMP(3)   NOT NULL,  
    currency_code       INT            NOT NULL,
    currency_code_with  INT            NOT NULL,  
    currency_with_div   FLOAT          NOT NULL  
);




CREATE TABLE "STV2025011438__DWH".global_metrics (
    date_update                    DATE            NOT NULL,
    currency_from                  INT             NOT NULL,   
    amount_total                   NUMERIC(18,2)   NOT NULL,
    cnt_transactions               INT             NOT NULL,
    avg_transactions_per_account   NUMERIC(18,2)   NULL,
    cnt_accounts_make_transactions INT             NOT NULL
)

ALTER TABLE "STV2025011438__DWH".global_metrics
ADD CONSTRAINT pk_global_metrics PRIMARY KEY(date_update, currency_from);
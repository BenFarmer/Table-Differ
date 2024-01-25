DROP TABLE IF EXISTS tabdiff.ex_1_secondary
;
CREATE TABLE tabdiff.ex_1_secondary (
    cust_id                 INT        NOT NULL,
    cust_name               VARCHAR    NOT NULL,
    status                  VARCHAR    NOT NULL,
    contract_start_date     DATE       NOT NULL,
    contract_end_date       DATE               ,
    contract_credits        INT        NOT NULL,
    contract_balance        INT        NOT NULL,
    latest_billing_period   DATE               ,
    record_written_at       DATE       NOT NULL
)
;

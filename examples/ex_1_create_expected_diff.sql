DROP TABLE IF EXISTS tabdiff.ex_1_expected_diff
;
CREATE TABLE tabdiff.ex_1_expected_diff (
    origin_cust_id                     INT        ,
    comparison_cust_id                 INT        ,
    origin_cust_name                   VARCHAR    ,
    comparison_cust_name               VARCHAR    ,
    origin_status                      VARCHAR    ,
    comparison_status                  VARCHAR    ,
    origin_contract_start_date         DATE       ,
    comparison_contract_start_date     DATE       ,
    origin_contract_end_date           DATE       ,
    comparison_contract_end_date       DATE       ,
    origin_contract_credits            INT        ,
    comparison_contract_credits        INT        ,
    origin_contract_balance            INT        ,
    comparison_contract_balance        INT        ,
    origin_latest_billing_period       DATE       ,
    comparison_latest_billing_period   DATE       
)
;

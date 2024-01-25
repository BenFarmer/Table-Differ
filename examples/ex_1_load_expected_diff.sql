INSERT INTO tabdiff.ex_1_expected_diff
    (origin_cust_id,
     comparison_cust_id,
     origin_cust_name,
     comparison_cust_name,
     origin_status,
     comparison_status,
     origin_contract_start_date,
     comparison_contract_start_date,
     origin_contract_end_date,
     comparison_contract_end_date,
     origin_contract_credits,
     comparison_contract_credits,
     origin_contract_balance,
     comparison_contract_balance,
     origin_latest_billing_period,
     comparison_latest_billing_period)
VALUES
    (1, 1, 'acme',   'acme' , 'active', 'active', '2023-01-01', '2023-01-01', '2023-12-31', '2023-12-31', 400, 400, 300, 340, '2023-11-01', '2023-12-01'),
    (2, 2, 'nasa',   'nasa' , 'active', 'active', '2023-01-01', '2023-01-01', '2023-12-31', '2023-12-31', 600, 600, 400, 420, '2023-11-01', '2023-12-01'),
    (3, 3, 'jpl',    'jpl'  , 'active', 'active', '2023-01-01', '2023-01-01', '2023-12-31', '2023-12-31', 600, 600, 400, 430, '2023-11-01', '2023-12-01'),
    (4,NULL,'disney',NULL,'inactive',NULL,'2022-01-01',NULL,'2022-12-31',NULL,300,NULL,280,NULL,'2022-12-01',NULL),
    (5, 5, 'ginsu',  'ginsu', 'active', 'active', '2022-01-01', '2022-01-01', '2024-12-31', '2024-12-31', 900, 900, 300, 320, '2023-11-01', '2023-12-01'),
    (NULL, 6,NULL ,'petrock', NULL    , 'active', NULL        , '2023-12-02', NULL        , '2024-12-31',NULL, 500,NULL,  15, NULL        , '2023-12-01')
;

INSERT INTO tabdiff.ex_1_secondary
    (cust_id,
     cust_name,
     status,
     contract_start_date,
     contract_end_date,
     contract_credits,
     contract_balance,
     latest_billing_period,
     record_written_at)
VALUES
    (1,'acme',    'active',   '2023-01-01', '2023-12-31', 400, 340, '2023-12-01', '2023-12-03'),
    (2,'nasa',    'active',   '2023-01-01', '2023-12-31', 600, 420, '2023-12-01', '2023-12-03'),
    (3,'jpl',     'active',   '2023-01-01', '2023-12-31', 600, 430, '2023-12-01', '2023-12-03'),
    (5,'ginsu',   'active',   '2022-01-01', '2024-12-31', 900, 320, '2023-12-01', '2023-12-03'),
    (6,'petrock', 'active',   '2023-12-02', '2024-12-31', 500, 015, '2023-12-01', '2023-12-03')
;

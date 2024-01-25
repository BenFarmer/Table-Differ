INSERT INTO tabdiff.ex_1_initial
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
    (1,'acme',    'active',   '2023-01-01', '2023-12-31', 400, 300, '2023-11-01', '2023-12-02'),
    (2,'nasa',    'active',   '2023-01-01', '2023-12-31', 600, 400, '2023-11-01', '2023-12-02'),
    (3,'jpl',     'active',   '2023-01-01', '2023-12-31', 600, 400, '2023-11-01', '2023-12-02'),
    (4,'disney',  'inactive', '2022-01-01', '2022-12-31', 300, 280, '2022-12-01', '2023-01-02'),
    (5,'ginsu',   'active',   '2022-01-01', '2024-12-31', 900, 300, '2023-11-01', '2023-12-02')
;

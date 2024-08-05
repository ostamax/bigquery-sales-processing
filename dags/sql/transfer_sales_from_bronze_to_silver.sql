-- We found out that data is not clean. i.e:
-- - purchase date: different date formats like 2022/09/01, 2022-09-10, 2022-Aug-30
-- - price contains '$' or 'USD' substrings

DELETE FROM `{{ params.project_id }}.silver.sales`
WHERE _logical_dt = "{{ ds }}"
;

INSERT `{{ params.project_id }}.silver.sales` (
    client_id,
    purchase_date,
    product_name,
    price,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT 
    CAST(CustomerId AS INT ),
    CASE
        WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-[a-zA-Z]{3}-\d{2}$')
            THEN PARSE_DATE('%Y-%b-%d', PurchaseDate)
        WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}\/\d{2}\/\d{2}$') 
            THEN PARSE_DATE('%Y/%m/%d', PurchaseDate)
        ELSE CAST(PurchaseDate AS DATE)
    END,
    Product, 
    CAST(regexp_replace(Price,'([^0-9])','') AS NUMERIC),

    _id,
    _logical_dt,
    CAST('{{ dag_run.start_date }}'AS TIMESTAMP) AS _job_start_dt
FROM `{{ params.project_id }}.bronze.sales`
WHERE CAST(_logical_dt as DATE) = "{{ ds }}";

-- Here we are handling dates from August 2022. We change month to September.
UPDATE `{{ params.project_id }}.silver.sales`
SET purchase_date = CAST(_logical_dt as DATE)
WHERE purchase_date <> CAST(_logical_dt as DATE);
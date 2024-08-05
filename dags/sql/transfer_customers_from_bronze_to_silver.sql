-- Dates are clean, so we don't need to pre-process them.

DELETE FROM `{{ params.project_id }}.silver.customers`
WHERE _logical_dt = "{{ ds }}"
;

INSERT `{{ params.project_id }}.silver.customers` (
    client_id, 
    first_name, 
    last_name, 
    email, 
    registration_date, 
    state,

    _logical_dt,
    _job_start_dt
)
SELECT
    CAST(Id AS INTEGER),
    FirstName,
    LastName,
    Email,
    CAST(RegistrationDate AS DATE),
    State,

    _logical_dt,
    CAST('{{ dag_run.start_date }}'AS TIMESTAMP) AS _job_start_dt
FROM `{{ params.project_id }}.bronze.customers`
WHERE CAST(RegistrationDate as DATE) = "{{ ds }}"; -- to get rig of duplicates in silver
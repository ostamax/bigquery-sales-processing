TRUNCATE TABLE `{{ params.project_id }}.gold.user_profiles_enriched`;

INSERT `{{ params.project_id }}.gold.user_profiles_enriched` (
    client_id,
    first_name,
    last_name,
    email,
    registration_date,
    state,
    birth_date,
    phone_number,
    _logical_dt,
    _job_start_dt
)
SELECT 
    ct.client_id,  
    COALESCE(ct.first_name, SPLIT(upt.full_name, ' ')[SAFE_ORDINAL(1)]) AS first_name,
    COALESCE(ct.last_name, SPLIT(upt.full_name, ' ')[SAFE_ORDINAL(2)]) AS last_name,
    COALESCE(ct.email, upt.email) AS email,
    ct.registration_date,
    COALESCE(ct.state, upt.state) AS state,
    CAST(upt.birth_date AS DATE),
    upt.phone_number,
    ct._logical_dt,
    CAST("{{ ds }}" AS TIMESTAMP)


FROM `de-07-maksym-ostapenko.silver.customers` AS ct
LEFT JOIN `de-07-maksym-ostapenko.silver.user_profiles` AS upt
ON  ct.email = upt.email;
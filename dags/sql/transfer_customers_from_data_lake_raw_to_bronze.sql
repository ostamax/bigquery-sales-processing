MERGE `{{ params.project_id }}.bronze.customers` AS Target
USING (
    SELECT Id,
        FirstName,
        LastName,
        Email,
        RegistrationDate,
        State 
    FROM customers_csv
    GROUP BY Id, FirstName, LastName, Email, RegistrationDate, State
) AS Source
ON Target.Id = Source.Id


-- For Inserts
WHEN NOT MATCHED BY Target THEN
    INSERT (Id, FirstName, LastName, Email, RegistrationDate, State, _logical_dt, _job_start_dt)
    VALUES (Id, FirstName, LastName, Email, RegistrationDate, State, CAST('{{ dag_run.logical_date }}' AS TIMESTAMP), CAST('{{ dag_run.start_date }}' AS TIMESTAMP))

-- For Updates
WHEN MATCHED THEN UPDATE SET
    Target.Id = Source.Id,
    Target.FirstName = Source.FirstName,
    Target.LastName = Source.LastName,
    Target.Email = Source.Email,
    Target.RegistrationDate = Source.RegistrationDate,
    Target.State  = Source.State,
    Target._logical_dt = CAST('{{ dag_run.logical_date }}' AS TIMESTAMP);

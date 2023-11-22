
  
    

    create or replace table `airflow-practice-spock-dev`.`token_holders`.`balances`
    
    

    OPTIONS()
    as (
      SELECT address, SUM(token_amount) AS balance
FROM (
    SELECT from_address AS address, -value AS token_amount
    FROM `airflow-practice-spock-dev`.`token_holders`.`transfers`
    UNION ALL
    SELECT to_address AS address, value AS token_amount
    FROM `airflow-practice-spock-dev`.`token_holders`.`transfers`
)
GROUP BY address
ORDER BY balance DESC
    );
  
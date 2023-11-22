SELECT 
    * 
FROM 
    `airflow-practice-spock-dev`.`token_holders`.`raw_logs`
WHERE 
    address = '0x37c997b35c619c21323f3518b9357914e8b99525'
AND
    topics[0] LIKE '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef%'
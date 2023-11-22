SELECT address, SUM(token_amount) AS balance
FROM (
    SELECT from_address AS address, -value AS token_amount
    FROM {{ ref('transfers') }}
    UNION ALL
    SELECT to_address AS address, value AS token_amount
    FROM {{ ref('transfers') }}
)
GROUP BY address
ORDER BY balance DESC

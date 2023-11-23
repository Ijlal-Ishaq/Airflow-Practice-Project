WITH decoded_token_transfers AS (
  SELECT
    `spock-main.token_holders.decode_token_transfer`(data, topics) AS decoded_data
  FROM {{ ref('transfer_logs') }} 
)

SELECT
  decoded_data.from_address AS from_address,
  decoded_data.to_address AS to_address,
  decoded_data.value AS value
FROM decoded_token_transfers

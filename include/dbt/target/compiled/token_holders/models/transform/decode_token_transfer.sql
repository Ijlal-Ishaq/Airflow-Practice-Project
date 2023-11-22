CREATE OR REPLACE FUNCTION `airflow-practice-spock-dev.token_holders.decode_token_transfer`(log_data STRING, topics ARRAY<STRING>) RETURNS STRUCT<from_address STRING, to_address STRING, value NUMERIC> LANGUAGE js
OPTIONS (library=["gs://blockchain-etl-bigquery/ethers.js"]) AS R"""
    var ethers = require('ethers');

    var tokenAbi = [
      {
        "inputs": [
          { "name": "from", "type": "address" },
          { "name": "to", "type": "address" },
          { "name": "value", "type": "uint256" }
        ],
        "name": "Transfer",
        "type": "event",
        "anonymous": false
      }
    ];

    var interface_instance = new ethers.utils.Interface(tokenAbi);

    try {
      var parsedLog = interface_instance.parseLog({topics: topics, data: log_data});
    } catch (e) {
        return null;
    }

    return {
      from_address: parsedLog.values.from,
      to_address: parsedLog.values.to,
      value: parsedLog.values.value
    };
""";
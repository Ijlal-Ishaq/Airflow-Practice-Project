from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
    
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig

from datetime import datetime
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['token_holders'],
)

def token_holders():

    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    query = """
    CREATE OR REPLACE VIEW token_holders.blocks AS
    SELECT *
    FROM `bigquery-public-data.crypto_ethereum.blocks`;

    CREATE OR REPLACE VIEW token_holders.transactions AS
    SELECT *
    FROM `bigquery-public-data.crypto_ethereum.transactions`;

    CREATE OR REPLACE VIEW token_holders.logs AS
    SELECT *
    FROM `bigquery-public-data.crypto_ethereum.logs`;

    CREATE OR REPLACE VIEW token_holders.traces AS
    SELECT *
    FROM `bigquery-public-data.crypto_ethereum.traces`;

    CREATE OR REPLACE FUNCTION `spock-main.token_holders.decode_token_transfer`(log_data STRING, topics ARRAY<STRING>) RETURNS STRUCT<from_address STRING, to_address STRING, value NUMERIC> LANGUAGE js
    OPTIONS (library=["gs://blockchain-etl-bigquery/ethers.js"]) AS '''
    var tokenAbi = [
      {
        "anonymous":false,
        "inputs": [
          {"indexed":true,"internalType":"address","name":"from","type":"address"},
          {"indexed":true,"internalType":"address","name":"to","type":"address"},
          {"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}
        ],
        "name":"Transfer",
        "type":"event"
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
      value: Number(parsedLog.values.value)
    };
    ''';
    """

    create_ref_tabels = BigQueryExecuteQueryOperator(
        task_id ='create_ref_tabels',
        sql = query,
        use_legacy_sql = False,
        gcp_conn_id = 'gcp',
    )

    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
    _check_transform = check_transform()

    snapshot = DbtTaskGroup(
        group_id='snapshot',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:snapshots']
        )
    )
    
    chain(
        begin,
        create_ref_tabels,
        transform,
        _check_transform,
        snapshot,
        end,
    )

token_holders()


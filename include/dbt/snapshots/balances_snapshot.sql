{% snapshot balances_snapshot %}

{{
    config(
      target_schema='token_holders',
      unique_key='address',
      strategy='check',
      check_cols=['balance'],
    )
}}

select * from {{ ref('balances') }}

{% endsnapshot %}
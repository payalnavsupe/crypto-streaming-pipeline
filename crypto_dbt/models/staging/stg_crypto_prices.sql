{{ config(materialized='view') }}

select
    id,
    bitcoin_usd,
    ethereum_usd,
    dogecoin_usd,
    timestamp as inserted_at
from {{ source('crypto_dbt', 'crypto_prices') }}

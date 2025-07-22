SELECT
    DATE_TRUNC('HOUR', inserted_at) AS hour,
    AVG(bitcoin_usd) AS avg_bitcoin_usd,
    AVG(ethereum_usd) AS avg_ethereum_usd,
    AVG(dogecoin_usd) AS avg_dogecoin_usd
FROM {{ ref('stg_crypto_prices') }}
GROUP BY 1
ORDER BY 1


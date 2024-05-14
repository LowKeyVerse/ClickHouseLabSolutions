SELECT formatReadableQuantity(count())
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
LIMIT 100;

SELECT *
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
WHERE crypto_name like 'bit_coin%' or crypto_name like 'Bit_coin'


SELECT AVG(volume)
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
where crypto_name = 'Bitcoin'

SELECT formatReadableQuantity(AVG(volume))
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')

--Trade for each crypto w/o using Trim
SELECT crypto_name as cryptoName, COUNT()
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
GROUP BY cryptoName
ORDER BY cryptoName

--Trade for each crypto using Trim
SELECT TRIM(crypto_name) as cryptoName, COUNT()
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
GROUP BY cryptoName
ORDER BY cryptoName

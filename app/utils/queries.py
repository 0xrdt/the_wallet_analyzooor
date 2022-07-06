"""
Aggregate of queries we will make to flipside.
"""

# TOKEN_NAME, START_DATE and CHAIN_NAME
tx_data = \
"""
SELECT 
  	date_trunc('hour', block_timestamp) as  "hour",
  	
  	avg(gas_price) as avg_gas_price,
  	max(gas_price) as max_gas_price,
  	min(gas_price) as min_gas_price,
  	(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY gas_price))  AS pct75th_gas_price,
  	(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gas_price))  AS median_gas_price,
  	(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY gas_price))  AS pct25th_gas_price,

  	avg(tx_fee) as avg_tx_fee,
    max(tx_fee) as max_tx_fee,
    min(tx_fee) as min_tx_fee,
    (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY tx_fee))  AS pct75th_tx_fee,
    (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee))  AS median_tx_fee,
    (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY tx_fee))  AS pct25th_tx_fee,
  
  	count(*) as number_of_txs,
  	count(case $TOKEN_NAME_VALUE when 0 then 0 else 1 end) as txs_with_native_token_transfer,
    count(case STATUS when 'STATUS' then 1 else null end) as successful_txs,
  	count(distinct from_address) as distinct_from_addresses,
  	count(distinct to_address) as distinct_to_addresses
FROM $CHAIN_NAME.core.fact_transactions 
WHERE block_timestamp>='$START_DATE'
GROUP BY 1
"""

# START_DATE and CHAIN_NAME
blocks_summary = \
"""
with txs_per_block as (
  SELECT 
  	block_timestamp,
  	count(*) as num_txs
  FROM $CHAIN_NAME.core.fact_transactions 
  WHERE block_timestamp>='$START_DATE'
  GROUP BY 1
)
SELECT 
  count(distinct block_timestamp) as num_blocks,
  max(num_txs) as max_num_txs,
  min(num_txs) as min_num_txs,
  avg(num_txs) as avg_num_txs,
  (PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY num_txs)) AS pct99th_num_txs,
  (PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY num_txs)) AS pct95th_num_txs,
  (PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY num_txs)) AS pct90th_num_txs,
  (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY num_txs)) AS pct75th_num_txs,
  (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY num_txs)) AS pct50th_num_txs,
  (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY num_txs)) AS pct25th_num_txs,
  (PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY num_txs)) AS pct10th_num_txs,
  (PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY num_txs)) AS pct05th_num_txs,
  (PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY num_txs)) AS pct01th_num_txs
FROM txs_per_block
"""

# START_DATE and CHAIN_NAME
time_between_blocks = \
"""
with unique_block_timestamps as (
  SELECT 
    distinct block_timestamp
  FROM $CHAIN_NAME.core.fact_transactions 
  WHERE block_timestamp>='$START_DATE'
  ORDER BY block_timestamp
),
  tmp as (
SELECT
  	block_timestamp,
  	-- lag(block_timestamp) over (order by block_timestamp) as prev_block_timestamp,
  	DATEDIFF(
  		'millisecond',
  		lag(block_timestamp) over (order by block_timestamp),
  		block_timestamp
    ) as time_between_blocks
FROM unique_block_timestamps
  )

SELECT 
	date_trunc('hour', block_timestamp) as "hour",
    max(time_between_blocks) as max_time_between_blocks,
    min(time_between_blocks) as min_time_between_blocks,
    avg(time_between_blocks) as avg_time_between_blocks,
    (PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY time_between_blocks)) AS pct99th_time_between_blocks,
    (PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY time_between_blocks)) AS pct95th_time_between_blocks,
    (PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY time_between_blocks)) AS pct90th_time_between_blocks,
    (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY time_between_blocks)) AS pct75th_time_between_blocks,
    (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY time_between_blocks)) AS pct50th_time_between_blocks,
    (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY time_between_blocks)) AS pct25th_time_between_blocks,
    (PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY time_between_blocks)) AS pct10th_time_between_blocks,
    (PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY time_between_blocks)) AS pct05th_time_between_blocks,
    (PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY time_between_blocks)) AS pct01th_time_between_blocks
FROM tmp
GROUP BY 1
"""

# START_DATE and CHAIN_NAME
agg_labeled_log_events = \
"""
SELECT
   -- ORIGIN_TO_ADDRESS,
   PROJECT_NAME,
   LABEL_TYPE,
   LABEL_SUBTYPE,
   count(distinct ORIGIN_FROM_ADDRESS) AS unique_users,
   count(*) AS log_counts
FROM $CHAIN_NAME.core.fact_event_logs logs
LEFT JOIN $CHAIN_NAME.core.dim_labels labels ON logs.ORIGIN_TO_ADDRESS=labels.ADDRESS
-- LEFT JOIN $CHAIN_NAME.core.dim_labels labels ON logs.ORIGIN_TO_ADDRESS=labels.ADDRESS
WHERE block_timestamp>='$START_DATE'
GROUP BY 1, 2, 3
"""

# START_DATE and CHAIN_NAME
agg_labeled_gas_guzzlers = \
"""
SELECT
   -- ORIGIN_TO_ADDRESS,
   PROJECT_NAME,
   LABEL_TYPE,
   LABEL_SUBTYPE,
   sum(tx_fee) as gas_guzzled_avax
FROM $CHAIN_NAME.core.fact_transactions txs
LEFT JOIN $CHAIN_NAME.core.dim_labels labels ON txs.TO_ADDRESS=labels.ADDRESS
-- LEFT JOIN $CHAIN_NAME.core.dim_labels labels ON logs.ORIGIN_TO_ADDRESS=labels.ADDRESS
WHERE block_timestamp>='$START_DATE'
GROUP BY 1, 2, 3
"""

# START_DATE, CHAIN_NAME_1 and CHAIN_NAME_2
addresses_similarities_between_chains = \
"""
with $CHAIN_NAME_1_addresses as (
  SELECT 
    	distinct from_address as address
  FROM $CHAIN_NAME_1.core.fact_transactions 
  WHERE block_timestamp>='$START_DATE'
  UNION ALL
  SELECT 
    	distinct to_address as address
  FROM $CHAIN_NAME_1.core.fact_transactions 
  WHERE block_timestamp>='$START_DATE'
),
 $CHAIN_NAME_2_addresses as (
  SELECT 
    	distinct from_address as address
  FROM $CHAIN_NAME_2.core.fact_transactions 
  WHERE block_timestamp>='$START_DATE'
  UNION ALL
  SELECT 
    	distinct to_address as address
  FROM $CHAIN_NAME_2.core.fact_transactions 
  WHERE block_timestamp>='$START_DATE'
 )

SELECT 
  count(*) as num_,
  '$CHAIN_NAME_2 addresses in $CHAIN_NAME_1' as desc_
FROM $CHAIN_NAME_1_addresses
WHERE address in (select address from $CHAIN_NAME_2_addresses)

UNION ALL 

SELECT 
  count(*) as num_,
  '$CHAIN_NAME_1 addresses in $CHAIN_NAME_2' as desc_
FROM $CHAIN_NAME_2_addresses
WHERE address in (select address from $CHAIN_NAME_1_addresses)

UNION ALL 

SELECT 
  count(*) as num_,
  'total $CHAIN_NAME_2 addresses' as desc_
FROM $CHAIN_NAME_2_addresses

UNION ALL 

SELECT 
  count(*) as num_,
  'total $CHAIN_NAME_1 addresses' as desc_
FROM $CHAIN_NAME_1_addresses
"""

# START_DATE, ERC20_CONTRACT_ADDRESSES
hourly_token_prices = \
"""
SELECT 
  * 
FROM ethereum.core.fact_hourly_token_prices 
WHERE "HOUR">='$START_DATE'
  AND token_address IN ($ERC20_CONTRACT_ADDRESSES)
"""

# START_DATE and CHAIN_NAME
agg_unlabeled_log_events = \
"""
SELECT
  ORIGIN_TO_ADDRESS,
  count(distinct ORIGIN_FROM_ADDRESS) AS unique_users,
  count(*) AS log_counts
FROM $CHAIN_NAME.core.fact_event_logs logs
WHERE block_timestamp>='$START_DATE'
GROUP BY 1
ORDER BY 3 DESC
LIMIT 50
"""

# START_DATE and CHAIN_NAME
agg_unlabeled_gas_guzzlers = \
"""
SELECT
   TO_ADDRESS,
   sum(tx_fee) as gas_guzzled_eth
FROM $CHAIN_NAME.core.fact_transactions txs
WHERE block_timestamp>='$START_DATE'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50
"""

token_transfers_per_wallet = \
"""
SELECT 
  *,
  'outgoing' as side
FROM $CHAIN_NAME.core.ez_token_transfers transfers
LEFT JOIN $CHAIN_NAME.core.dim_labels labels ON transfers.ORIGIN_TO_ADDRESS=labels.address
WHERE ORIGIN_FROM_ADDRESS = '$WALLET_ADDRESS' AND block_timestamp>='$START_DATE'
UNION ALL
SELECT 
  *,
  'incoming' as side
FROM $CHAIN_NAME.core.ez_token_transfers transfers
LEFT JOIN $CHAIN_NAME.core.dim_labels labels ON transfers.ORIGIN_FROM_ADDRESS=labels.address
WHERE ORIGIN_TO_ADDRESS = '$WALLET_ADDRESS' AND block_timestamp>='$START_DATE'
"""

native_token_transfers_per_wallet = \
"""
SELECT 
  *,
  'outgoing' as side 
FROM $CHAIN_NAME.core.ez_$TOKEN_NAME_transfers transfers
LEFT JOIN $CHAIN_NAME.core.dim_labels labels ON transfers.ORIGIN_TO_ADDRESS=labels.address
WHERE ORIGIN_FROM_ADDRESS = '$WALLET_ADDRESS' AND block_timestamp>='$START_DATE'
UNION ALL
SELECT 
  *,
  'incoming' as side 
FROM $CHAIN_NAME.core.ez_$TOKEN_NAME_transfers transfers
LEFT JOIN $CHAIN_NAME.core.dim_labels labels ON transfers.ORIGIN_FROM_ADDRESS=labels.address
WHERE ORIGIN_TO_ADDRESS = '$WALLET_ADDRESS' AND block_timestamp>='$START_DATE'
"""

transactions_per_wallet = \
"""
SELECT 
  *,
  'outgoing' as side
FROM $CHAIN_NAME.core.fact_transactions transactions
LEFT JOIN $CHAIN_NAME.core.dim_labels labels ON transactions.to_address=labels.address
WHERE FROM_ADDRESS = '$WALLET_ADDRESS' AND block_timestamp>='$START_DATE'
UNION ALL
SELECT 
  *,
  'incoming' as side
FROM $CHAIN_NAME.core.fact_transactions transactions
LEFT JOIN $CHAIN_NAME.core.dim_labels labels ON transactions.from_address=labels.address
WHERE TO_ADDRESS = '$WALLET_ADDRESS' AND block_timestamp>='$START_DATE'
"""

# START_DATE, CHAIN_NAME
distinct_addresses = \
"""
SELECT 
  	count(distinct from_address) as distinct_from_addresses,
  	count(distinct to_address) as distinct_to_addresses
FROM $CHAIN_NAME.core.fact_transactions 
WHERE block_timestamp>='$START_DATE' AND block_timestamp>='$START_DATE'
"""

# START_DATE, CHAIN_NAME
avg_tx_per_address = \
"""
SELECT 
  count(*)/count(distinct from_address) as avg_tx_per_address
FROM $CHAIN_NAME.core.fact_transactions
WHERE block_timestamp>='$START_DATE'
"""

# START_DATE, CHAIN_NAME
bots = \
"""
SELECT
  distinct from_address,
  count(*)/count(distinct date_trunc('day', block_timestamp)) as avg_daily_tx_per_address,
  count(*) as total_txs
FROM $CHAIN_NAME.core.fact_transactions txs
WHERE block_timestamp>='$START_DATE'
group by 1
HAVING avg_daily_tx_per_address>20
"""

erc20_balances_per_address = \
"""
SELECT 
  * 
FROM flipside_prod_db.ethereum.erc20_balances 
WHERE USER_ADDRESS='$WALLET_ADDRESS' AND BALANCE_DATE>='$START_DATE'
"""

wallet_label = \
"""
SELECT 
  *
FROM $CHAIN_NAME.core.dim_labels
WHERE ADDRESS='$WALLET_ADDRESS'
"""
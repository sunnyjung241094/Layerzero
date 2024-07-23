WITH send AS (
  SELECT
    call_block_time,
    _dstChainId,
    CASE _dstChainId
      WHEN 42161 THEN 110 -- Arbitrum
      WHEN 43114 THEN 106 -- Avalanche C-Chain
      WHEN 56 THEN 102 -- Binance Smart Chain
      WHEN 1 THEN 101 -- Ethereum Mainnet
      WHEN 10 THEN 111 -- Optimism
      WHEN 137 THEN 109 -- Polygon
      WHEN 250 THEN 112 -- Fantom
    END AS _srcChainId,
    sender,
    active_month_count
  FROM (
    SELECT
      call_block_time,
      _dstChainId,
      sender,
      dense_rank() OVER(PARTITION BY sender ORDER BY date_trunc('YEAR', block_time), month(block_time)) AS active_month_count
    FROM (
      SELECT call_block_time, _dstChainId, call_tx_hash
      FROM layerzero_arbitrum.Endpoint_call_send
      WHERE call_success = true
      UNION ALL
      SELECT call_block_time, _dstChainId, call_tx_hash
      FROM layerzero_avalanche_c.Endpoint_call_send
      WHERE call_success = true
      UNION ALL
      SELECT call_block_time, _dstChainId, call_tx_hash
      FROM layerzero_bnb.Endpoint_call_send
      WHERE call_success = true
      UNION ALL
      SELECT call_block_time, _dstChainId, call_tx_hash
      FROM layerzero_ethereum.Endpoint_call_send
      WHERE call_success = true
      UNION ALL
      SELECT call_block_time, _dstChainId, call_tx_hash
      FROM layerzero_optimism.Endpoint_call_send
      WHERE call_success = true
      UNION ALL
      SELECT call_block_time, _dstChainId, call_tx_hash
      FROM layerzero_polygon.Endpoint_call_send
      WHERE call_success = true
      UNION ALL
      SELECT call_block_time, _dstChainId, call_tx_hash
      FROM layerzero_fantom_endpoint_fantom.Endpoint_call_send
      WHERE call_success = true
    ) AS all_calls
    JOIN (
      SELECT hash, "from" as sender, block_time
      FROM arbitrum.transactions
      UNION ALL
      SELECT hash, "from" as sender, block_time
      FROM avalanche_c.transactions
      UNION ALL
      SELECT hash, "from" as sender, block_time
      FROM bnb.transactions
      UNION ALL
      SELECT hash, "from" as sender, block_time
      FROM ethereum.transactions
      UNION ALL
      SELECT hash, "from" as sender, block_time
      FROM optimism.transactions
      UNION ALL
      SELECT hash, "from" as sender, block_time
      FROM polygon.transactions
      UNION ALL
      SELECT hash, "from" as sender, block_time
      FROM fantom.transactions
    ) AS all_txs ON all_calls.call_tx_hash = all_txs.hash
  ) AS send
)
SELECT
  CASE
    WHEN active_month_count >= 12 THEN '12+ month'
    WHEN active_month_count >= 9 THEN '9+ month'
    WHEN active_month_count >= 6 THEN '6+ month'
    WHEN active_month_count >= 2 THEN '2+ month'
    ELSE '1+ month'
  END AS active_month_range,
  count(DISTINCT sender) AS user_count
FROM
  send
GROUP BY
  1
ORDER BY
  user_count DESC

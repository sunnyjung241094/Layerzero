WITH
  send AS (
    SELECT
      call_block_time,
      _dstChainId,
      110 as _srcChainId,
      sender
    FROM
      layerzero_arbitrum.Endpoint_call_send arb
      LEFT JOIN (
        SELECT
          hash,
          "from" as sender
        FROM
          arbitrum.transactions
      ) arbt ON arb.call_tx_hash = arbt.hash
    WHERE
      call_success = true
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      106 as _srcChainId,
      sender
    FROM
      layerzero_avalanche_c.Endpoint_call_send ava
      LEFT JOIN (
        SELECT
          hash,
          "from" as sender
        FROM
          avalanche_c.transactions
      ) avat ON ava.call_tx_hash = avat.hash
    WHERE
      call_success = true
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      102 as _srcChainId,
      sender
    FROM
      layerzero_bnb.Endpoint_call_send bnb
      LEFT JOIN (
        SELECT
          hash,
          "from" as sender
        FROM
          bnb.transactions
      ) bnbt ON bnb.call_tx_hash = bnbt.hash
    WHERE
      call_success = true
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      101 as _srcChainId,
      sender
    FROM
      layerzero_ethereum.Endpoint_call_send eth
      LEFT JOIN (
        SELECT
          hash,
          "from" as sender
        FROM
          ethereum.transactions
      ) etht ON eth.call_tx_hash = etht.hash
    WHERE
      call_success = true
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      111 as _srcChainId,
      sender
    FROM
      layerzero_optimism.Endpoint_call_send opt
      LEFT JOIN (
        SELECT
          hash,
          "from" as sender
        FROM
          optimism.transactions
      ) optt ON opt.call_tx_hash = optt.hash
    WHERE
      call_success = true
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      109 as _srcChainId,
      sender
    FROM
      layerzero_polygon.Endpoint_call_send pol
      LEFT JOIN (
        SELECT
          hash,
          "from" as sender
        FROM
          polygon.transactions
      ) polt ON pol.call_tx_hash = polt.hash
    WHERE
      call_success = true
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      112 as _srcChainId,
      sender
    FROM
      layerzero_fantom_endpoint_fantom.Endpoint_call_send fan
      LEFT JOIN (
        SELECT
          hash,
          "from" as sender
        FROM
          fantom.transactions
      ) fant ON fan.call_tx_hash = fant.hash
    WHERE
      call_success = true
  ),
  user_count AS (
    SELECT
      sender,
      count(*) as trans_count
    FROM
      send
    GROUP BY
      1
  ),
  user_threshold AS (
    SELECT
      CASE
        WHEN trans_count >= 100 THEN '100 Tx'
        WHEN trans_count >= 50 THEN '50 Tx'
        WHEN trans_count >= 30 THEN '30 ~ 49 Tx'
        WHEN trans_count >= 20 THEN '20 ~ 29 Tx'
        WHEN trans_count >= 10 THEN '10 ~ 19 Tx'
        WHEN trans_count >= 2 THEN '2 ~ 9 Tx'
        WHEN trans_count >= 1 THEN '1 Tx'
      END AS threshold,
      COUNT(*) AS count_of_users
    FROM
      user_count
      WHERE trans_count > 0
    GROUP BY
      1
  )
SELECT
  *
FROM
  user_threshold;

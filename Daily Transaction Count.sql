-- This query has been migrated to run on DuneSQL.
-- If you need to change back because you’re seeing issues, use the Query History view to restore the previous version.
-- If you notice a regression, please email dunesql-feedback@dune.com.

WITH
  send AS (
    SELECT
      call_block_time,
      _dstChainId,
      110 AS _srcChainId
    FROM
      layerzero_arbitrum.Endpoint_call_send
    WHERE
      call_success
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      106 AS _srcChainId
    FROM
      layerzero_avalanche_c.Endpoint_call_send
    WHERE
      call_success
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      102 AS _srcChainId
    FROM
      layerzero_bnb.Endpoint_call_send
    WHERE
      call_success
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      101 AS _srcChainId
    FROM
      layerzero_ethereum.Endpoint_call_send
    WHERE
      call_success
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      111 AS _srcChainId
    FROM
      layerzero_optimism.Endpoint_call_send
    WHERE
      call_success
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      109 AS _srcChainId
    FROM
      layerzero_polygon.Endpoint_call_send
    WHERE
      call_success
    UNION ALL
    SELECT
      call_block_time,
      _dstChainId,
      112 AS _srcChainId
    FROM
      layerzero_fantom_endpoint_fantom.Endpoint_call_send
    WHERE
      call_success
  ),
  send2 AS (
    SELECT
      call_block_time,
      CASE
        WHEN _dstChainId = 101 THEN 'ethereum'
        WHEN _dstChainId = 102 THEN 'binance'
        WHEN _dstChainId = 106 THEN 'avalanche'
        WHEN _dstChainId = 108 THEN 'aptos'
        WHEN _dstChainId = 109 THEN 'polygon'
        WHEN _dstChainId = 110 THEN 'arbitrum'
        WHEN _dstChainId = 111 THEN 'optimism'
        WHEN _dstChainId = 112 THEN 'fantom'
        WHEN _dstChainId = 114 THEN 'swimmer'
        WHEN _dstChainId = 115 THEN 'DFK'
        WHEN _dstChainId = 116 THEN 'harmony'
        WHEN _dstChainId = 126 THEN 'moonbeam'
        WHEN _dstChainId = 125 THEN 'celo'
        WHEN _dstChainId = 118 THEN 'dexalot'
        WHEN _dstChainId = 138 THEN 'fuse'
        WHEN _dstChainId = 145 THEN 'gnosis'
        WHEN _dstChainId = 150 THEN 'klaytn'
        WHEN _dstChainId = 151 THEN 'metis'
        WHEN _dstChainId = 152 THEN 'intain'
        WHEN _dstChainId = 153 THEN 'coredao'
        WHEN _dstChainId = 155 THEN 'okx'
        WHEN _dstChainId = 1 THEN 'ethereum'
        WHEN _dstChainId = 2 THEN 'binance'
        WHEN _dstChainId = 6 THEN 'avalanche'
        WHEN _dstChainId = 8 THEN 'aptos'
        WHEN _dstChainId = 9 THEN 'polygon'
        WHEN _dstChainId = 10 THEN 'arbitrum'
        WHEN _dstChainId = 11 THEN 'optimism'
        WHEN _dstChainId = 12 THEN 'fantom'
        WHEN _dstChainId = 14 THEN 'swimmer'
        WHEN _dstChainId = 15 THEN 'DFK'
        WHEN _dstChainId = 16 THEN 'harmony'
        WHEN _dstChainId = 26 THEN 'moonbeam'
        WHEN _dstChainId = 25 THEN 'celo'
        WHEN _dstChainId = 18 THEN 'dexalot'
        WHEN _dstChainId = 38 THEN 'fuse'
        WHEN _dstChainId = 45 THEN 'gnosis'
        WHEN _dstChainId = 50 THEN 'klaytn'
        WHEN _dstChainId = 51 THEN 'metis'
        WHEN _dstChainId = 52 THEN 'intain'
        WHEN _dstChainId = 53 THEN 'coredao'
        WHEN _dstChainId = 55 THEN 'okx'
        ELSE cast(_dstChainId as varchar)
      END AS dstChain,
      CASE
        WHEN _srcChainId = 101 THEN 'ethereum'
        WHEN _srcChainId = 102 THEN 'binance'
        WHEN _srcChainId = 106 THEN 'avalanche'
        WHEN _srcChainId = 108 THEN 'aptos'
        WHEN _srcChainId = 109 THEN 'polygon'
        WHEN _srcChainId = 110 THEN 'arbitrum'
        WHEN _srcChainId = 111 THEN 'optimism'
        WHEN _srcChainId = 112 THEN 'fantom'
        WHEN _srcChainId = 114 THEN 'swimmer'
        WHEN _srcChainId = 115 THEN 'DFK'
        WHEN _srcChainId = 116 THEN 'harmony'
        WHEN _srcChainId = 126 THEN 'moonbeam'
        WHEN _srcChainId = 125 THEN 'celo'
        WHEN _srcChainId = 118 THEN 'dexalot'
        WHEN _srcChainId = 138 THEN 'fuse'
        WHEN _srcChainId = 145 THEN 'gnosis'
        WHEN _srcChainId = 150 THEN 'klaytn'
        WHEN _srcChainId = 151 THEN 'metis'
        WHEN _srcChainId = 152 THEN 'intain'
        WHEN _srcChainId = 153 THEN 'coredao'
        WHEN _srcChainId = 155 THEN 'okx'
        ELSE cast(_srcChainId as varchar)
      END AS srcChain
    FROM
      send
  )
SELECT
  *,
  SUM(num_daily_tx) OVER (
    ORDER BY
      day
  ) AS num_cumul_tx
FROM
  (
    SELECT
      DATE_TRUNC('day', call_block_time) AS day,
      COUNT(*) AS num_daily_tx
    FROM
      send2
    GROUP BY
      1
  )

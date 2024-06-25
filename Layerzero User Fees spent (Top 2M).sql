with TOP1 AS (SELECT 
"from",
  SUM(N) AS "# Transactions",
  MAX(CASE WHEN source = 'ETH' THEN usd ELSE 0 END) AS Ethereum,
  MAX(CASE WHEN source = 'op' THEN usd ELSE 0 END) AS Optimism,
  MAX(CASE WHEN source = 'gnosis' THEN usd ELSE 0 END) AS Gnosis,
  MAX(CASE WHEN source = 'ftm' THEN usd ELSE 0 END) AS 	Fantom,
  MAX(CASE WHEN source = 'avax' THEN usd ELSE 0 END) AS Avalanche,
  MAX(CASE WHEN source = 'arb' THEN usd ELSE 0 END) AS Arbitrum,
  MAX(CASE WHEN source = 'bnb' THEN usd ELSE 0 END) AS BSC,
  MAX(CASE WHEN source = 'polygon' THEN usd ELSE 0 END) AS Polygon,
  MAX(CASE WHEN source = 'base' THEN usd ELSE 0 END) AS Base,
  MAX(CASE WHEN source = 'zksync' THEN usd ELSE 0 END) AS Zksync,
  MAX(CASE WHEN source = 'scroll' THEN usd ELSE 0 END) AS Scroll,
  MAX(CASE WHEN source = 'zora' THEN usd ELSE 0 END) AS Zora,
  MAX(CASE WHEN source = 'celo' THEN usd ELSE 0 END) AS Celo,
  MAX(CASE WHEN source = 'zkevm' THEN usd ELSE 0 END) AS Zkevm,
  MAX(CASE WHEN source = 'linea' THEN usd ELSE 0 END) AS Linea,
  SUM(usd) AS total_amount
FROM (
  SELECT 'ETH' AS source, usd, "from", N
  FROM query_2676950
  UNION ALL
  SELECT 'op' AS source, usd, "from", N
  FROM query_2687914
  UNION ALL
  SELECT 'gnosis' AS source, usd, "from", N
  FROM query_2687982
  UNION ALL
  SELECT 'ftm' AS source, usd, "from", N
  FROM query_2687970
  UNION ALL
  SELECT 'avax' AS source, usd, "from", N
  FROM query_2687933
  UNION ALL
  SELECT 'arb' AS source, usd, "from", N
  FROM query_2687868
  UNION ALL
  SELECT 'bnb' AS source, usd, "from", N
  FROM query_2687938
  UNION ALL
  SELECT 'polygon' AS source, usd, "from", N
  FROM query_2687920
  UNION ALL
  SELECT 'base' AS source, usd, "from", N
  FROM query_3465898
  UNION ALL
  SELECT 'zksync' AS source, usd, "from", N
  FROM query_3465914
  UNION ALL
  SELECT 'scroll' AS source, usd, "from", N
  FROM query_3467019
  UNION ALL
  SELECT 'zora' AS source, usd, "from", N
  FROM query_3467042
  UNION ALL
  SELECT 'celo' AS source, usd, "from", N
  FROM query_3467053
  UNION ALL
  SELECT 'zkevm' AS source, usd, "from", N
  FROM query_3720804
  UNION ALL
  SELECT 'linea' AS source, usd, "from", N
  FROM query_3720793
) AS subquery
GROUP BY "from"
ORDER BY total_amount DESC 
LIMIT 2000000)

select row_number() over (ORDER BY total_amount DESC ) as "Rank", "from",  "# Transactions",
 Ethereum,
 Optimism,
 Gnosis,
Fantom,
Avalanche,
 Arbitrum,
 BSC,
 Polygon,
 Base,
 Zksync,
 Scroll,
 Zora,
 Celo,
 Zkevm,
 Linea,
 total_amount
 from TOP1

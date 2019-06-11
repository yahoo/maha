

SELECT "Campaign Name", CASE WHEN clicks = 0 THEN 0.0 ELSE spend / clicks END AS "Average CPC", avg_pos AS "Average Position", impressions AS "Impressions"
FROM(
SELECT getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING)) "mang_campaign_name", (CASE WHEN SUM(impressions) = 0 THEN 0.0 ELSE SUM(avg_pos * impressions) / (SUM(impressions)) END) AS avg_pos, SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(spend) AS spend
FROM(SELECT campaign_id, SUM(impressions) impressions, SUM(clicks) clicks, SUM(spend) spend
FROM s_stats_fact
WHERE (account_id = 12345) AND (stats_date >= '2019-05-31' AND stats_date <= '2019-06-07')
GROUP BY campaign_id

       )
ssf0
LEFT OUTER JOIN (
SELECT campaign_name AS mang_campaign_name, id c1_id
FROM campaing_hive
WHERE ((load_time = '%DEFAULT_DIM_PARTITION_PREDICTATE%' ) AND (shard = 'all' )) AND (advertiser_id = 12345)
)
c1
ON
ssf0.campaign_id = c1.c1_id

GROUP BY getCsvEscapedString(CAST(NVL(c1.mang_campaign_name, '') AS STRING))
)

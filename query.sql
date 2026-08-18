SELECT
    name,
    substr(ts, 1, 7) AS "year-month",
    AVG(((close_stock - open_stock) / open_stock) * 100) AS avg_monthly_pct_change
FROM ranxin_jiang_project02_datastream_bucket
GROUP BY name, substr(ts, 1, 7)
ORDER BY name, "year-month";

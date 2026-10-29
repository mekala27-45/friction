-- statement
-- What the build used in the workspace, from the billing system table (it lags by some hours).
-- save: billing
SELECT usage_date, billing_origin_product AS product, sku_name, usage_unit,
       coalesce(usage_metadata.job_id, usage_metadata.dlt_pipeline_id, '') AS resource,
       round(sum(usage_quantity), 4) AS quantity
FROM system.billing.usage
WHERE usage_date >= DATE'2026-10-28'
GROUP BY ALL
ORDER BY usage_date, product, resource

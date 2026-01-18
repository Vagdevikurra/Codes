DROP TABLE IF EXISTS dm_ib_dev.wealth_insights_accounts_fact_pbi;

CREATE TABLE dm_ib_dev.wealth_insights_accounts_fact_pbi
STORED AS PARQUET
AS
SELECT *
FROM dm_ib_dev.wealth_insights_accounts_fact
WHERE wealth_accounts_cnt > 0
   OR investpath_accounts_cnt > 0;
DROP TABLE IF EXISTS dm_ib_dev.wealth_insights_customer_dim_pbi;

CREATE TABLE dm_ib_dev.wealth_insights_customer_dim_pbi
STORED AS PARQUET
AS
SELECT d.*
FROM dm_ib_dev.wealth_insights_customer_dim d
JOIN (
  SELECT DISTINCT rcif_number
  FROM dm_ib_dev.wealth_insights_accounts_fact_pbi
) f
ON d.rcif_number = f.rcif_number;
DROP TABLE IF EXISTS dm_ib_dev.company_digital_kpis_6mo;

CREATE TABLE dm_ib_dev.company_digital_kpis_6mo
STORED AS PARQUET
AS
SELECT
  month_dt,
  COUNT(DISTINCT CASE WHEN digital_active_ind = 1 THEN rcif_number END) AS company_digital_active_rcif_cnt,
  COUNT(DISTINCT rcif_number) AS company_total_rcif_cnt,
  SUM(digital_enrollments_cnt) AS company_digital_enrollments_sum
FROM dm_ib_dev.wealth_insights_accounts_fact
GROUP BY month_dt
ORDER BY month_dt;
DROP TABLE IF EXISTS dm_ib_dev.company_digital_kpis_6mo_ibn;

CREATE TABLE dm_ib_dev.company_digital_kpis_6mo_ibn
STORED AS PARQUET
AS
SELECT
  month_dt,
  SUM(digital_enrollments_cnt) AS company_digital_enrollments_sum,
  SUM(CASE WHEN digital_active_ind = 1 THEN digital_enrollments_cnt ELSE 0 END) AS company_digital_active_enrollments_sum
FROM dm_ib_dev.wealth_insights_accounts_fact
GROUP BY month_dt
ORDER BY month_dt;



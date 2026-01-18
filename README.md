# 1) Base rows (month grain column added)
spark.sql("""
CREATE OR REPLACE TEMP VIEW digital_base AS
SELECT
  TRUNC(CAST(dbm.ods_business_dt AS DATE), 'MM') AS month_dt,
  CAST(dbm.rcif_customer_nbr AS STRING)          AS rcif_number,
  CAST(dbm.ibn AS STRING)                        AS relt_ibn,
  CAST(dbm.olb_last_login_date AS DATE)          AS olb_login_dt,
  CAST(dbm.mob_last_login_date AS DATE)          AS mob_login_dt
FROM dm_ib.digital_banking_master dbm
WHERE CAST(dbm.ods_business_dt AS DATE) >= add_months(trunc(current_date(),'MM'), -6)
  AND CAST(dbm.ods_business_dt AS DATE) <  trunc(current_date(),'MM')
  AND dbm.rcif_customer_nbr IS NOT NULL
""")

# 2) Aggregate to (month_dt, rcif, relt_ibn)
spark.sql("""
CREATE OR REPLACE TEMP VIEW digital_agg AS
SELECT
  month_dt,
  rcif_number,
  relt_ibn,
  MAX(olb_login_dt) AS lst_login_olb,
  MAX(mob_login_dt) AS lst_login_mob
FROM digital_base
GROUP BY month_dt, rcif_number, relt_ibn
""")

# 3) Derive flags using month_dt (NOT ods_business_dt)
spark.sql("""
CREATE OR REPLACE TEMP VIEW digital_monthly AS
SELECT
  month_dt,
  rcif_number,
  relt_ibn,
  lst_login_olb,
  lst_login_mob,

  CASE
    WHEN datediff(month_dt, lst_login_mob) <= 90 THEN 'Mobile Active'
    ELSE 'Non Mobile Active'
  END AS mobile_active_flag,

  CASE
    WHEN lst_login_mob IS NULL THEN 'Non Mobile User'
    ELSE 'Mobile User'
  END AS mobile_flag,

  CASE
    WHEN datediff(month_dt, lst_login_olb) <= 90 THEN 'OLB Active'
    ELSE 'Non OLB Active'
  END AS olb_active_flag,

  CASE
    WHEN lst_login_olb IS NULL THEN 'Non OLB User'
    ELSE 'OLB User'
  END AS olb_flag,

  CASE
    WHEN (datediff(month_dt, lst_login_mob) <= 90)
      OR (datediff(month_dt, lst_login_olb) <= 90)
      THEN 'Digital Active'
    ELSE 'Non Digital Active'
  END AS digitally_active_flag
FROM digital_agg
""")

# 4) Customer-month rollup (same as before)
spark.sql("""
CREATE OR REPLACE TEMP VIEW digital_customer_month AS
SELECT
  month_dt,
  rcif_number,
  MAX(CASE WHEN digitally_active_flag = 'Digital Active' THEN 1 ELSE 0 END) AS digital_active_ind,
  MAX(CASE WHEN mobile_active_flag    = 'Mobile Active'  THEN 1 ELSE 0 END) AS mobile_active_ind,
  MAX(CASE WHEN olb_active_flag       = 'OLB Active'     THEN 1 ELSE 0 END) AS olb_active_ind,
  MAX(CASE WHEN mobile_flag           = 'Mobile User'    THEN 1 ELSE 0 END) AS mobile_user_ind,
  MAX(CASE WHEN olb_flag              = 'OLB User'       THEN 1 ELSE 0 END) AS olb_user_ind,
  COUNT(DISTINCT relt_ibn) AS digital_enrollments_cnt
FROM digital_monthly
GROUP BY month_dt, rcif_number
""")

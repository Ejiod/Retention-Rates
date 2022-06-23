WITH cohorts AS (
    SELECT user_id,
MIN(DATE_TRUNC('Month',login_date)) OVER (PARTITION BY user_id) as cohort_month, ----###first_applied_date
EXTRACT(YEAR FROM AGE(DATE_TRUNC('Month',login_date),MIN(DATE_TRUNC('Month',login_date)) 
OVER (PARTITION BY user_id)))*12 + EXTRACT(MONTHS FROM AGE(DATE_TRUNC('Month',login_date) ,
MIN(DATE_TRUNC('Month',login_date)) OVER (PARTITION BY user_id))) AS retention_month 
FROM users_login_table 
where approval_datetime::date >= '2021-01-01'
),

 customers AS (SELECT
cohort_month,
retention_month,
 COUNT(DISTINCT user_id) as number_of_customer
FROM cohorts
GROUP BY 1,2),

month_cohorts as (
SELECT
cohort_month::DATE,
retention_month,
number_of_customer,
number_of_customer / FIRST_VALUE(number_of_customer) OVER (PARTITION BY cohort_month ORDER BY retention_month)::decimal*100 AS retention_rate
FROM customers
ORDER BY cohort_month ASC, retention_month ASC

)
SELECT '2021-07-01' AS cohort,
max(case when retention_month = 0 and cohort_month = '2021-07-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2021-07-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2021-07-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2021-07-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2021-07-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2021-07-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2021-07-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2021-07-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2021-07-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2021-07-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2021-07-01' then retention_rate end) as m10,
max(case when retention_month = 11 and cohort_month ='2021-07-01' then retention_rate end) as m11
 from month_cohorts group by 1
UNION all
SELECT '2021-08-01' AS cohort,
max(case when retention_month = 0 and cohort_month = '2021-08-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2021-08-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2021-08-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2021-08-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2021-08-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2021-08-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2021-08-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2021-08-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2021-08-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2021-08-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2021-08-01' then retention_rate end) as m10,
max(case when retention_month = 11 and cohort_month ='2021-08-01' then retention_rate end) as m11
 from month_cohorts group by 1
UNION ALL
 SELECT '2021-09-01' AS cohort,
max(case when retention_month = 0 and cohort_month = '2021-09-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2021-09-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2021-09-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2021-09-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2021-09-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2021-09-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2021-09-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2021-09-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2021-09-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2021-09-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2021-09-01' then retention_rate end) as m10,
max(case when retention_month = 11 and cohort_month ='2021-09-01' then retention_rate end) as m11
 from month_cohorts group by 1
UNION ALL

 SELECT '2021-10-01' AS cohort,
max(case when retention_month = 0 and cohort_month = '2021-10-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2021-10-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2021-10-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2021-10-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2021-10-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2021-10-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2021-10-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2021-10-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2021-10-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2021-10-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2021-10-01' then retention_rate end) as m10,
max(case when retention_month = 11 and cohort_month ='2021-10-01' then retention_rate end) as m11
 from month_cohorts group by 1

 UNION ALL

 SELECT '2021-11-01' AS cohort,
max(case when retention_month = 0 and cohort_month = '2021-11-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2021-11-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2021-11-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2021-11-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2021-11-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2021-11-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2021-11-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2021-11-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2021-11-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2021-11-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2021-11-01' then retention_rate end) as m10,
max(case when retention_month = 11 and cohort_month ='2021-11-01' then retention_rate end) as m11
 from month_cohorts group by 1

 UNION ALL 

 SELECT '2021-12-01' AS cohort,
max(case when retention_month = 0 and cohort_month = '2021-12-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2021-12-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2021-12-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2021-12-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2021-12-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2021-12-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2021-12-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2021-12-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2021-12-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2021-12-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2021-12-01' then retention_rate end) as m10,
max(case when retention_month = 12 and cohort_month ='2021-12-01' then retention_rate end) as m11
 from month_cohorts group by 1

  UNION ALL 

 SELECT '2022-01-01' AS cohort, 
max(case when retention_month = 0 and cohort_month = '2022-01-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2022-01-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2022-01-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2022-01-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2022-01-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2022-01-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2022-01-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2022-01-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2022-01-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2022-01-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2022-01-01' then retention_rate end) as m10,
max(case when retention_month = 11 and cohort_month ='2022-01-01' then retention_rate end) as m11
 from month_cohorts group by 1

UNION ALL

 SELECT '2022-02-01' AS cohort,
max(case when retention_month = 0 and cohort_month = '2022-02-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2022-02-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2022-02-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2022-02-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2022-02-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2022-02-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2022-02-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2022-02-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2022-02-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2022-02-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2022-02-01' then retention_rate end) as m10,
max(case when retention_month = 11 and cohort_month ='2022-02-01' then retention_rate end) as m11
 from month_cohorts group by 1

 UNION ALL

 SELECT '2022-03-01' AS cohort,
max(case when retention_month = 0 and cohort_month = '2022-03-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2022-03-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2022-03-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2022-03-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2022-03-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2022-03-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2022-03-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2022-03-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2022-03-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2022-03-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2022-03-01' then retention_rate end) as m10,
max(case when retention_month = 11 and cohort_month ='2022-03-01' then retention_rate end) as m11
 from month_cohorts group by 1
UNION ALL

 SELECT '2022-04-01' AS cohort,
max(case when retention_month = 0 and cohort_month = '2022-04-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2022-04-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2022-04-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2022-04-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2022-04-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2022-04-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2022-04-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2022-04-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2022-04-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2022-04-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2022-04-01' then retention_rate end) as m10,
max(case when retention_month = 11 and cohort_month ='2022-04-01' then retention_rate end) as m11
 from month_cohorts group by 1

 UNION ALL

 SELECT '2022-05-01' AS cohort,
max(case when retention_month = 0 and cohort_month = '2022-05-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2022-05-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2022-05-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2022-05-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2022-05-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2022-05-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2022-05-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2022-05-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2022-05-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2022-05-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2022-05-01' then retention_rate end) as m10,
max(case when retention_month = 11 and cohort_month ='2022-05-01' then retention_rate end) as m11
 from month_cohorts group by 1

 UNION ALL

 SELECT '2022-06-01' AS cohort,
max(case when retention_month = 0 and cohort_month = '2022-06-01' then retention_rate end) as m0,
max(case when retention_month = 1 and cohort_month = '2022-06-01' then retention_rate end) as m1,
max(case when retention_month = 2 and cohort_month = '2022-06-01' then retention_rate end) as m2,
max(case when retention_month = 3 and cohort_month = '2022-06-01' then retention_rate end) as m3,
max(case when retention_month = 4 and cohort_month = '2022-06-01' then retention_rate end) as m4,
max(case when retention_month = 5 and cohort_month = '2022-06-01' then retention_rate end) as m5,
max(case when retention_month = 6 and cohort_month = '2022-06-01' then retention_rate end) as m6,
max(case when retention_month = 7 and cohort_month = '2022-06-01' then retention_rate end) as m7,
max(case when retention_month = 8 and cohort_month = '2022-06-01' then retention_rate end) as m8,
max(case when retention_month = 9 and cohort_month = '2022-06-01' then retention_rate end) as m9,
max(case when retention_month = 10 and cohort_month ='2022-06-01' then retention_rate end) as m10,
max(case when retention_month = 11 and cohort_month ='2022-06-01' then retention_rate end) as m11
 from month_cohorts group by 1
 


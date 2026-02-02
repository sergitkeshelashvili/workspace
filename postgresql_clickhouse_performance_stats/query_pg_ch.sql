SELECT
   created_date,
   COUNT(*) AS transactions_count,
   SUM(total_amount) AS daily_total,
   SUM(SUM(total_amount)) OVER (ORDER BY created_date) AS cumulative_total
FROM (
   SELECT
       created_at::date AS created_date,
       total_amount
   FROM ecommerce_transactions
) t
GROUP BY created_date
ORDER BY created_date;

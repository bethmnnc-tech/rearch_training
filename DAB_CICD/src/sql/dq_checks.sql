-- Data quality checks; fails the task if any row returns.
SELECT * FROM gold_daily_sales WHERE revenue < 0;

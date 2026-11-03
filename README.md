# SQL Optimization Helper for MySQL

A small script for finding slow queries and checking whether a fix actually helped. It reads a query's execution plan with `EXPLAIN`, flags full table scans, suggests indexes for the columns in the `WHERE` clause, and times the original query against your rewritten one.

## What it does

1. **Execution plan check.** Runs `EXPLAIN` on a query and looks for tables read with `type = ALL` and no usable key.
2. **Index suggestions.** Pulls the column names out of the `WHERE` clause and suggests an index on them for each table that is fully scanned.
3. **Before and after timing.** Runs the original and the optimized query and prints both times and the percentage improvement.
4. **Scheduled checks.** Every minute, runs a small health check in a transaction (the row count of a `users` table and the InnoDB buffer pool data size) using the `schedule` library, until you stop it with Ctrl+C.

## Run it

```bash
pip install mysql-connector-python schedule
```

1. Fill in the connection details in `main()` in `sql_opt_v2.py`.
2. Replace the placeholder queries in `sql_analysis_demo()` (`[column_name]`, `[TABLE]`, `[WHERE_OPTIONS]`) with the query you want to check and your improved version.
3. Run:
   ```bash
   python sql_opt_v2.py
   ```

## Limits

The `WHERE` parsing is a simple regular expression, so it handles plain conditions like `column = value` or `column > value` joined by `AND` and `OR`. Joins, subqueries and functions on columns need a manual look at the plan.

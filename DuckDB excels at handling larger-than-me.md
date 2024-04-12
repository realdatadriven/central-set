DuckDB excels at handling larger-than-memory workloads  by spilling data to disk, also known as out-of-core processing. Here's how to configure the connection for this functionality:

**Automatic Spilling:**

By default, DuckDB automatically spills data to disk when encountering memory limitations. No special configuration is needed for this behavior. 

**Optimizing Spilling:**

While automatic spilling works well, you can optimize performance with these tips:

1. **Fast Disk:** Ensure you have a fast disk (SSD preferred) for handling temporary data during spills. Refer to DuckDB's environment guide for more details [Disk in DuckDB documentation].
2. **Limited Memory:** If your system has limited memory, consider reducing the number of threads used by DuckDB. You can achieve this using the `SET threads = <number_of_threads>;` command before running your queries.

**Advanced Configuration (Optional):**

While not typically required, DuckDB offers some advanced configuration options for spill behavior:

1. **Temporary Directory:** DuckDB creates a temporary directory (`<database_file_name>.tmp`) by default for spilling. You can customize this location using the `PRAGMA data_directory = '<path_to_directory>'` statement.

For a deeper dive into DuckDB's out-of-core processing and performance tuning, refer to the official documentation:

* Tuning Workloads: [DuckDB Tuning Workloads]
* Environment: [DuckDB Environment]


```sql
SET threads = 4;
EXPORT DATABASE 'bak' (FORMAT PARQUET);
```
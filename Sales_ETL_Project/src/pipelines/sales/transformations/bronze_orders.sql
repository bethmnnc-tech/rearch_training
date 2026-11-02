CREATE OR REFRESH STREAMING TABLE bronze_orders
AS SELECT *, _metadata.file_path AS src_file
FROM STREAM read_files('${source_path}/orders', format => 'json');

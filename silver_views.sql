CREATE TABLE silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://fatima.arshad/datalake/silver_views'
    ) AS SELECT article, views, rank, date
         FROM bronze_views 
         WHERE date IS NOT NULL;

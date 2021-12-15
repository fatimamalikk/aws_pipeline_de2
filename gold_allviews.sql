CREATE TABLE fatimamalikk_homework.gold_allviews
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://fatima.arshad/datalake/gold_allviews'
    ) AS SELECT article, SUM(views) AS total_top_views, MIN(rank) AS top_rank, COUNT(*) AS ranked_days
         FROM fatimamalikk_homework.silver_views
         GROUP BY article;
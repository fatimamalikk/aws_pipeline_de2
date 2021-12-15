CREATE EXTERNAL TABLE
fatimamalikk_homework.bronze_views (    
    article STRING,
    views INT,
    rank INT,
    date DATE,
    retrieved_at TIMESTAMP) 
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    LOCATION 's3://fatima.arshad/datalake/views/';
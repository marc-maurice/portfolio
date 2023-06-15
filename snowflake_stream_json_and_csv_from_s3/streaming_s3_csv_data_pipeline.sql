use database ecommerce_db;

create schema ecommerce_dev;

create or replace table lineitem cluster by (L_SHIPDATE) AS select * from ECOMMERCE_DB.ECOMMERCE_LIV.LINEITEM limit 1;
truncate table lineitem;


CREATE FILE FORMAT csv_load_format
    TYPE = 'CSV' 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER =1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO';

drop stg_lineitem_csv_dev;
-- Create a stage for lineitem table  ---
create or replace stage stg_lineitem_csv_dev
storage_integration = aws_sf_data
url = 's3://marc-snowflake-data1/ecommerce_dev/lineitem/lineitem_csv_data/'
file_format = csv_load_format;

list @stg_lineitem_csv_dev;

copy into lineitem
from @stg_lineitem_csv_dev 
file_format = csv_load_format 
ON_ERROR = ABORT_STATEMENT;

select count(1) from lineitem;




CREATE OR REPLACE FILE FORMAT json_load_format TYPE = 'JSON' ;

create or replace stage stg_lineitem_json_dev
storage_integration = aws_sf_data
url = 's3://marc-snowflake-data1/ecommerce_dev/lineitem/lineitem_json_data/'
file_format = json_load_format;

list @stg_lineitem_json_dev;

copy into lineitem from @stg_lineitem_json_dev ON_ERROR = ABORT_STATEMENT;

select $1 from @stg_lineitem_json_dev;

create table lineitem_raw_json (src variant );

copy into lineitem_raw_json from @stg_lineitem_json_dev ON_ERROR = ABORT_STATEMENT;

select * from lineitem_raw_json;

select 
    SRC:L_ORDERKEY,
    SRC:L_PARTKEY,
    SRC:L_SUPPKEY,
    SRC:L_LINENUMBER,
    SRC:L_QUANTITY,
    SRC:L_EXTENDEDPRICE,
    SRC:L_DISCOUNT,
    SRC:L_TAX,
    SRC:L_RETURNFLAG,
    SRC:L_LINESTATUS,
    SRC:L_SHIPDATE,
    SRC:L_COMMITDATE,
    SRC:L_RECEIPTDATE,
    SRC:L_SHIPINSTRUCT,
    SRC:L_SHIPMODE,
    SRC:L_COMMENT
from 
    lineitem_raw_json ;


insert into lineitem
    select 
    $1:L_ORDERKEY,
    $1:L_PARTKEY,
    $1:L_SUPPKEY,
    $1:L_LINENUMBER,
    $1:L_QUANTITY,
    $1:L_EXTENDEDPRICE,
    $1:L_DISCOUNT,
    $1:L_TAX,
    $1:L_RETURNFLAG::varchar,
    $1:L_LINESTATUS::varchar,
    $1:L_SHIPDATE::varchar,
    $1:L_COMMITDATE::varchar,
    $1:L_RECEIPTDATE::varchar,
    $1:L_SHIPINSTRUCT::varchar,
    $1:L_SHIPMODE::varchar,
    $1:L_COMMENT::varchar
from 
    @stg_lineitem_json_dev ;




CREATE OR REPLACE FILE FORMAT parquet_load_format TYPE = 'parquet';

create or replace stage stg_lineitem_parquet_dev
storage_integration = aws_sf_data
url = 's3://marc-snowflake-data1/ecommerce_dev/lineitem/lineitem_parquet_data/'
file_format = parquet_load_format;

list @stg_lineitem_parquet_dev;


select $1 from @stg_lineitem_parquet_dev limit 10;


insert into lineitem
select 
    $1:L_ORDERKEY,
    $1:L_PARTKEY,
    $1:L_SUPPKEY,
    $1:L_LINENUMBER,
    $1:L_QUANTITY,
    $1:L_EXTENDEDPRICE,
    $1:L_DISCOUNT,
    $1:L_TAX,
    $1:L_RETURNFLAG::varchar,
    $1:L_LINESTATUS::varchar,
    $1:L_SHIPDATE::varchar,
    $1:L_COMMITDATE::varchar,
    $1:L_RECEIPTDATE::varchar,
    $1:L_SHIPINSTRUCT::varchar,
    $1:L_SHIPMODE::varchar,
    $1:L_COMMENT::varchar
from 
    @stg_lineitem_parquet_dev ;





create or replace stage stg_lineitem_csv_dev
storage_integration = aws_sf_data
url = 's3://marc-snowflake-data1/ecommerce_dev/lineitem/lineitem_snowpipe/'
file_format = csv_load_format;

list @stg_lineitem_csv_dev;


create or replace pipe lineitem_pipe auto_ingest=true as
copy into lineitem from @stg_lineitem_csv_dev ON_ERROR = continue;

show pipes;

select count(1) from lineitem;

select * from information_schema.load_history where table_name = 'LINEITEM' order by last_load_time;



CREATE FILE FORMAT csv_load_format
    TYPE = 'CSV' 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER =1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO';

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";


INSERT INTO LINEITEM
SELECT *
FROM ECOMMERCE_LIV.LINEITEM LIMIT 100000;


-- Extract/Unload data  ---
copy into s3://marc-snowflake-data1/unloaded_data/lineitem/
from
(
  select * from "ECOMMERCE_DB"."ECOMMERCE_DEV"."LINEITEM"
)
storage_integration=aws_sf_data
single=false
file_format = CSV_LOAD_FORMAT;


copy into s3://marc-snowflake-data1/unloaded_data/lineitem_partitioned/
from
(
  select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" limit 100000
)
partition by L_SHIPDATE
storage_integration=aws_sf_data
single=false
file_format = csv_load_format;


copy into s3://marc-snowflake-data1/unloaded_data/lineitem_parquet/
from
(
  select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
  limit 1000000
)
storage_integration=aws_sf_data
single=false
file_format = parquet_load_format;

copy into s3://marc-snowflake-data1/unloaded_data/lineitem_parquet_partitioned/
from
(
  select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
  limit 1000000
)
partition by L_SHIPDATE::varchar
storage_integration=aws_sf_data
single=false
file_format = parquet_load_format;


copy into s3://marc-snowflake-data1/unloaded_data/lineitem_json/
from
(
  select 
  object_construct(
  'L_ORDERKEY',L_ORDERKEY,
  'L_PARTKEY',L_PARTKEY,
  'L_SUPPKEY',L_SUPPKEY,
  'L_LINENUMBER',L_LINENUMBER,
   'L_QUANTITY',L_QUANTITY,
   'L_EXTENDEDPRICE',L_EXTENDEDPRICE,
  'L_DISCOUNT',L_DISCOUNT,
    'L_TAX',L_TAX,
    'L_RETURNFLAG',L_RETURNFLAG,
    'L_LINESTATUS',L_LINESTATUS,
    'L_SHIPDATE',L_SHIPDATE,
    'L_COMMITDATE',L_COMMITDATE,
    'L_RECEIPTDATE',L_RECEIPTDATE,
    'L_SHIPINSTRUCT',L_SHIPINSTRUCT,
    'L_SHIPMODE',L_SHIPMODE,
    'L_COMMENT',L_COMMENT
  )
  from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
  limit 1000000
)
storage_integration=aws_sf_data
single=false
file_format = json_load_format;



















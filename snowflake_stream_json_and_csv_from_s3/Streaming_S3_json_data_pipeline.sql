-- in this script, I will show you how I build a streaming data pipeline of json data from S3 to Snowflake using witchcraft and wizardry ... or just sql and stuff. This is the code which allows me to show you how when I drop a json file into my s3 bucket, it magically appears in snowflake. 

-- let us start by using the accountadmin role to make sure the sysadmin can do the things
use role accountadmin;

-- create your integration object
CREATE or REPLACE STORAGE INTEGRATION aws_sf_data
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::79675------:role/snowflake-aws-readonly-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://marc-snowflake-data1');

-- let the sysadmin use the object
grant usage on integration aws_sf_data to role sysadmin;

-- give some more power to the sysadmin
grant execute task on account to role sysadmin;

-- okay, the sysadmin is about to be all omnicient
grant create stage on schema "ECOMMERCE_DB"."ECOMMERCE_DEV" to role sysadmin;

-- i am now the all powerful sysadmin, 
use role sysadmin;

-- this is where we will do the things
use schema "ECOMMERCE_DB"."ECOMMERCE_DEV";

-- tell me about this object. need this so we can connect s3 to snowflake
desc INTEGRATION aws_sf_data;

-- file formats are a necessary thing to allow us to say what file format we are using. 
CREATE OR REPLACE FILE FORMAT json_load_format 
    TYPE = 'JSON';

-- let us make the stage where we will parade our data across
create or replace stage stg_lineitem_json_dev
storage_integration = aws_sf_data
url = 's3://marc-snowflake-data1/streams_dev/'
file_format = json_load_format;

-- show me the goods
list @stg_lineitem_json_dev;

-- lets start bringing the stuff in!
create or replace table lineitem_raw_json (src variant );

select * from lineitem_raw_json limit 10;

CREATE OR REPLACE STREAM lineitem_std_stream ON TABLE lineitem_raw_json;

select * from lineitem_std_stream limit 10;

-- let us use a task to set up the stream
create or replace task lineitem_load_tsk 
warehouse = etl_xl
schedule = '1 minute'
when system$stream_has_data('lineitem_std_stream')
as 
merge into lineitem as li 
using 
(
   select 
        SRC:L_ORDERKEY as L_ORDERKEY,
        SRC:L_PARTKEY as L_PARTKEY,
        SRC:L_SUPPKEY as L_SUPPKEY,
        SRC:L_LINENUMBER as L_LINENUMBER,
        SRC:L_QUANTITY as L_QUANTITY,
        SRC:L_EXTENDEDPRICE as L_EXTENDEDPRICE,
        SRC:L_DISCOUNT as L_DISCOUNT,
        SRC:L_TAX as L_TAX,
        SRC:L_RETURNFLAG as L_RETURNFLAG,
        SRC:L_LINESTATUS as L_LINESTATUS,
        SRC:L_SHIPDATE as L_SHIPDATE,
        SRC:L_COMMITDATE as L_COMMITDATE,
        SRC:L_RECEIPTDATE as L_RECEIPTDATE,
        SRC:L_SHIPINSTRUCT as L_SHIPINSTRUCT,
        SRC:L_SHIPMODE as L_SHIPMODE,
        SRC:L_COMMENT as L_COMMENT
    from 
        lineitem_std_stream
    where metadata$action='INSERT'
) as li_stg
on li.L_ORDERKEY = li_stg.L_ORDERKEY and li.L_PARTKEY = li_stg.L_PARTKEY and li.L_SUPPKEY = li_stg.L_SUPPKEY
when matched then update 
set 
    li.L_PARTKEY = li_stg.L_PARTKEY,
    li.L_SUPPKEY = li_stg.L_SUPPKEY,
    li.L_LINENUMBER = li_stg.L_LINENUMBER,
    li.L_QUANTITY = li_stg.L_QUANTITY,
    li.L_EXTENDEDPRICE = li_stg.L_EXTENDEDPRICE,
    li.L_DISCOUNT = li_stg.L_DISCOUNT,
    li.L_TAX = li_stg.L_TAX,
    li.L_RETURNFLAG = li_stg.L_RETURNFLAG,
    li.L_LINESTATUS = li_stg.L_LINESTATUS,
    li.L_SHIPDATE = li_stg.L_SHIPDATE,
    li.L_COMMITDATE = li_stg.L_COMMITDATE,
    li.L_RECEIPTDATE = li_stg.L_RECEIPTDATE,
    li.L_SHIPINSTRUCT = li_stg.L_SHIPINSTRUCT,
    li.L_SHIPMODE = li_stg.L_SHIPMODE,
    li.L_COMMENT = li_stg.L_COMMENT
when not matched then insert 
(
    L_ORDERKEY,
    L_PARTKEY,
    L_SUPPKEY,
    L_LINENUMBER,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_TAX,
    L_RETURNFLAG,
    L_LINESTATUS,
    L_SHIPDATE,
    L_COMMITDATE,
    L_RECEIPTDATE,
    L_SHIPINSTRUCT,
    L_SHIPMODE,
    L_COMMENT
) 
values 
(
    li_stg.L_ORDERKEY,
    li_stg.L_PARTKEY,
    li_stg.L_SUPPKEY,
    li_stg.L_LINENUMBER,
    li_stg.L_QUANTITY,
    li_stg.L_EXTENDEDPRICE,
    li_stg.L_DISCOUNT,
    li_stg.L_TAX,
    li_stg.L_RETURNFLAG,
    li_stg.L_LINESTATUS,
    li_stg.L_SHIPDATE,
    li_stg.L_COMMITDATE,
    li_stg.L_RECEIPTDATE,
    li_stg.L_SHIPINSTRUCT,
    li_stg.L_SHIPMODE,
    li_stg.L_COMMENT
);

show tasks;

-- tasks need a reminder to start since their default is to not be started. makes sense.
alter task lineitem_load_tsk resume;
show tasks;

-- here we go!
copy into lineitem_raw_json from @stg_lineitem_json_dev ON_ERROR = ABORT_STATEMENT;

select * from lineitem_raw_json limit 10;

select * from lineitem_std_stream limit 10;

select * from lineitem limit 10;
-- we did it - our data moves!

-- letus take a closer look at what happened
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 100));

-- prior to creating the pipe we need to clear everything out so we have a fresh start!
truncate lineitem;
truncate lineitem_raw_json;
select * from lineitem_raw_json limit 10;
select * from lineitem_std_stream limit 10;
select * from lineitem limit 10;
select * from @stg_lineitem_json_dev;

-- let's rebuild!
create view json_stg (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) as 
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
    @stg_lineitem_json_dev; 

    
copy into lineitem from @stg_lineitem_json_dev;

create or replace pipe lineitem_pipe auto_ingest=true as
copy into lineitem_raw_json from @stg_lineitem_json_dev;

show pipes;

-- to retrieve the notification channel arn for the pipe to connect to the SQS event notification
-- I think my work is done here


select * from lineitem_raw_json limit 10;

select * from lineitem limit 10;

select count(1) from lineitem limit 10;

select * from @stg_lineitem_json_dev limit 10;

alter table lineitem set change_tracking = true;





select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 100));
    
    
select *
    from table(information_schema.pipe_usage_history(
    date_range_start=>dateadd('hour',-3,current_timestamp()),
    pipe_name=>'lineitem_pipe'));
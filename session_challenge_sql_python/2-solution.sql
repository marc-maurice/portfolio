/// Create table
create or replace TABLE pageviews_analytics (
  event_id INT NOT NULL,
  anonymous_id VARCHAR(255) NOT NULL,
  event_time TIMESTAMP NOT NULL,
  url VARCHAR(255) NOT NULL
);

/// Insert data
INSERT INTO pageviews_analytics (event_id, anonymous_id, event_time, url) VALUES
  (1, '123456', '2022-01-01 10:00:00', 'https://example.com/'),
  (2, '789012', '2022-01-01 10:05:00', 'https://example.com/product-page'),
  (3, '345678', '2022-01-01 10:10:00', 'https://example.com/product-page'),
  (4, '123456', '2022-01-01 10:15:00', 'https://example.com/product-page?color=blue'),
  (5, '345678', '2022-01-01 10:20:00', 'https://example.com/product-page'),
  (6, '123456', '2022-01-01 10:25:00', 'https://example.com/cart'),
  (7, '345678', '2022-01-01 10:30:00', 'https://example.com/'),
  (8, '789012', '2022-01-01 10:35:00', 'https://example.com/cart'),
  (9, '123456', '2022-01-01 10:40:00', 'https://example.com/product-page?color=red'),
  (10, '345678', '2022-01-01 10:45:00', 'https://example.com/'),
  (11, '123456', '2022-01-01 10:50:00', 'https://example.com/product-page?color=red'),
  (12, '789012', '2022-01-01 10:55:00', 'https://example.com/'),
  (13, '345678', '2022-01-01 11:00:00', 'https://example.com/product-page?color=blue'),
  (14, '123456', '2022-01-01 11:05:00', 'https://example.com/product-page?color=green'),
  (15, '789012', '2022-01-01 11:10:00', 'https://example.com/'),
  (16, '345678', '2022-01-01 11:15:00', 'https://example.com/cart'),
  (17, '123456', '2022-01-01 11:20:00', 'https://example.com/product-page'),
  (18, '789012', '2022-01-01 11:25:00', 'https://example.com/'),
  (19, '345678', '2022-01-01 11:30:00', 'https://example.com/product-page?color=blue'),
  (20, '123456', '2022-01-01 11:35:00', 'https://example.com/product-page'),
  (21, '789012', '2022-01-01 11:40:00', 'https://example.com/cart'),
  (22, '345678', '2022-01-01 11:45:00', 'https://example.com/product-page?color=red');

/// 
create or replace table pageviews_analytics2 as
   select * 
    from (
        select 
        event_id,
        anonymous_id,
        url,
        event_time,
        lag_time,
        TIMESTAMPDIFF(min, lag_time,event_time) as delta_time
        from 
            (select *,
            lag(event_time,1) OVER(Partition By anonymous_id Order By event_time) as lag_time
    from pageviews_analytics
  )) a;

-- option 1: continue and complete creating the session ids in snowflake
-- option 2: extract the data at this point from pageviews_analytics2 and complete in python. session_challenge.py for the python solution
  
update pageviews_analytics2
set delta_time = 999
where delta_time is null;

create or replace table pageviews_analytics3 as
select * 
from pageviews_analytics2
where delta_time >= 30;

alter table pageviews_analytics3 add session_id varchar;

update pageviews_analytics3
set session_id = uuid_string();

create or replace table pageviews_analytics4 as
select p2.*,p3.session_id
from pageviews_analytics2 p2
left outer join pageviews_analytics3 p3
on p2.event_id = p3.event_id;

create or replace table pageviews_sessions as
select
pg4.event_id,
pg4.anonymous_id,
pg4.url,
pg4.event_time,
pg4.lag_time,
pg4.delta_time,
case when pg4.session_id is not null then pg4.session_id else lag(session_id) ignore nulls over (order by anonymous_id, event_id) end as session_id
from pageviews_analytics4 pg4;

select *
from pageviews_sessions
order by anonymous_id, event_id;

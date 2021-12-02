# ksqldbWorkshop

Create a Stream and a Table
==============================

CREATE STREAM pageviews_stream (
    viewtime int, 
    userid varchar, 
    pageid varchar
) 
WITH (kafka_topic='pageviews_topic', value_format='JSON');

CREATE TABLE users (
    userid varchar PRIMARY KEY, 
    registertime bigint, 
    gender varchar, 
    regionid varchar
) 
WITH (KAFKA_TOPIC='users_topic', VALUE_FORMAT='JSON');

Create a Persistent Query
=========================
CREATE STREAM pageviews_enriched AS
    SELECT users.userid AS userid, 
           regionid, 
           gender, 
           viewtime, 
           pageid
    FROM pageviews_stream
    LEFT JOIN users
    ON pageviews_stream.userid = users.userid
EMIT CHANGES;

Aggregate Data
================
CREATE TABLE number_of_times_page_viewed AS
    SELECT pageid,
           COUNT(userid) AS total_users
    FROM pageviews_stream
    GROUP BY pageid
EMIT CHANGES;


CREATE TABLE total_view_time_minutes AS
    SELECT pageid,
           SUM(viewtime) AS total_view_time
    FROM pageviews_enriched
    GROUP BY pageid
    EMIT CHANGES;


Time Windows
===========

CREATE TABLE pages_viewed_today AS
    SELECT pageid,
           COUNT(*) AS views_in_5mins
    FROM pageviews_enriched
    WINDOW TUMBLING (SIZE 5 MINUTES)
    GROUP BY pageid
    EMIT CHANGES;


CREATE TABLE users_to_monitor AS
    SELECT TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss Z') AS WINDOW_START,
           TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss Z') AS WINDOW_END,
           userid,
           COUNT(*) AS quantity
    FROM pageviews_enriched
    WINDOW TUMBLING (SIZE 5 MINUTES)
    GROUP BY userid
    HAVING COUNT(*) > 10;


Pull Queries
==============
SELECT * FROM users_to_monitor
     WHERE QUANTITY > 50;

select * from TOTAL_VIEW_TIME_MINUTES
    where PAGEID = 'Page_66';

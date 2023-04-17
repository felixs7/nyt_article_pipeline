CREATE SCHEMA SOURCE;


DROP TABLE IF EXISTS SOURCE.NYT_TOP_ARTICLES;
CREATE TABLE SOURCE.NYT_TOP_ARTICLES (
    id BIGINT,
    published_date DATE,
    section CHAR(500) ,
    adx_keywords VARCHAR(max),
    byline VARCHAR,
    type CHAR(500),
    title VARCHAR(max),
    abstract VARCHAR(max)

);
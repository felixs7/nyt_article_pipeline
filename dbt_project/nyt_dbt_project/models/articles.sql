-- {{ config(materialized='view') }}
{{ config(materialized='incremental') }}

WITH raw_articles as (
    SELECT * FROM {{ source ('nyt_source', 'NYT_TOP_ARTICLES') }}
)
, all_articles AS (
SELECT id as article_id,
        published_date,
        section,
        adx_keywords,
        byline,
        type,
        title,
        abstract,
        COALESCE(timestamp_api_call, 20230401000000) as timestamp_api_call,
        TO_DATE(SUBSTRING(TEXT(COALESCE(timestamp_api_call, 20230401000000)), 1, 8), 'YYYYMMDD') AS timestamp_api_call_day
FROM raw_articles
{% if is_incremental() %}
        WHERE COALESCE(raw_articles.timestamp_api_call, 20230401000000) >
           (SELECT MAX(timestamp_api_call) FROM {{ this }})
{% endif %}
),
-- Assign a number for each Article & day partition. If the API is called 5 times a day return only the latest article for this day
-- However, when the same article is returned on another day include it in the reporting table.
-- this allows analysis on popular articles that appear multiple times in the top 20.
articles_per_day as (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY article_id, timestamp_api_call_day  ORDER BY timestamp_api_call DESC) as row_number
        FROM all_articles       
)

SELECT  article_id,
        published_date,
        section,
        adx_keywords,
        byline,
        type,
        title,
        abstract,
        timestamp_api_call,
        timestamp_api_call_day,
        GETDATE() AS etl_runtime
FROM articles_per_day
WHERE row_number = 1
--and article_id = 100000008835091
--ORDER BY timestamp_api_call, article_id, published_date
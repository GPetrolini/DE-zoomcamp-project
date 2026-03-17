{{ config(materialized='table') }}

with staging_data as (
    select * from {{ ref('stg_vgsales') }}
)

select
    platform,
    release_year,
    count(game_name) as total_games_released,
    sum(global_sales) as total_global_sales,
    ARRAY_AGG(genre ORDER BY global_sales DESC LIMIT 1)[OFFSET(0)] as top_selling_genre

from staging_data

group by 
    platform,
    release_year

order by 
    release_year desc, 
    total_global_sales desc
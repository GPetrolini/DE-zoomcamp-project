{{ config(materialized='view') }}

with raw_video_games as (
    select * from `de-zoomcamp-484312.video_game_data.vgsales_native_table`
)

select
    Rank as rank,
    Name as game_name,
    Platform as platform,
    Year as release_year,
    Genre as genre,
    Publisher as publisher,
    NA_Sales as na_sales,
    EU_Sales as eu_sales,
    JP_Sales as jp_sales,
    Other_Sales as other_sales,
    Global_Sales as global_sales

from raw_video_games

where Year is not null 
  and Year != 'N/A'
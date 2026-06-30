-- Data Lakehouse — dbt Staging Model
-- Masimbonge Portfolio

{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
    select
        *,
        current_timestamp() as _loaded_at
    from {{ source('raw', 'weather_stage') }}
),

renamed as (
    select
        parse_json($1):time::array           as time_series,
        parse_json($1):hourly::object        as hourly_data,
        parse_json($1):latitude::float       as latitude,
        parse_json($1):longitude::float      as longitude,
        parse_json($1):timezone::string      as timezone,
        _loaded_at
    from source
)

select * from renamed

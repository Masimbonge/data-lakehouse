-- Project 2: Data Lakehouse — dbt Mart Model
-- Masimbonge Portfolio
-- This is the clean, analytics-ready table served to dashboards

{{ config(
    materialized='table',
    schema='marts',
    cluster_by=['date_day']
) }}

with weather_staged as (
    select * from {{ ref('stg_weather') }}
),

flattened as (
    select
        f.value::string                                  as observation_time,
        weather_staged.latitude,
        weather_staged.longitude,
        weather_staged.timezone,
        -- Edit: add more fields from your raw data
        parse_json(weather_staged.hourly_data):temperature_2m[f.index]::float as temperature_celsius,
        _loaded_at
    from weather_staged,
    lateral flatten(input => weather_staged.time_series) f
),

final as (
    select
        observation_time::date                          as date_day,
        observation_time::timestamp                     as observation_timestamp,
        latitude,
        longitude,
        timezone,
        temperature_celsius,
        round(temperature_celsius * 9/5 + 32, 1)       as temperature_fahrenheit,
        _loaded_at,
        -- Data lineage metadata
        '{{ invocation_id }}'                           as dbt_run_id,
        current_timestamp()                             as dbt_updated_at
    from flattened
    where temperature_celsius is not null
)

select * from final

{{ config(
    materialized = 'incremental',
) }}

with pipeline_executions as (
    select *
     from {{ source('jaffle_shop', 'tbl_pipeline_executions') }}
    {% if is_incremental() %}
    where dt_init > (select max(max_start_datetime) from {{ this }})
    {% endif %}
),

aggregated_pipeline_executions as (
    select
        date(dt_init) as start_date,
        count(*) as executions,
        min(dt_init) as min_start_datetime,
        max(dt_init) as max_start_datetime,
        min(dt_end) as min_end_datetime,
        max(dt_end) as max_end_datetime
    from pipeline_executions
    group by 1
    ORDER BY date(dt_init) ASC
    LIMIT 1 {# for learning reasons, limit the new records to 1 only #}
)
select * from aggregated_pipeline_executions
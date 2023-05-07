with power_consumption as (
    select * from {{ ref("actual_power_consumption") }}
),

power_consumption_hourly as (
    select
        hour(start_date) as hour,
        avg(total_network_load_MWh) as total_network_load_MWh,
        avg(residual_load_MWh) as residual_load_MWh
    from power_consumption
    group by 1
),

final as (
    select
        hour,
        total_network_load_MWh,
        residual_load_MWh,
        total_network_load_MWh - residual_load_MWh as renewables_load_MWh
    from power_consumption_hourly
)

select * from final
with power_gen as (
    select * from {{ ref('actual_power_gen_yearly') }}
),

final as (
    select
        year,
        biomass_MWh + waterpower_MWh + wind_offshore_MWh + wind_onshore_MWh + solar_MWh as renewables_MWh,
        nuclear_MWh + brown_coal_MWh + hard_coal_MWh + gas_MWh + pump_storage_MWh + misc_conventional_MWh as conventional_MWh
    from power_gen
)

select * from final
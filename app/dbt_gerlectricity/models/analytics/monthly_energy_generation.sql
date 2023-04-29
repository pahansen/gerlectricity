with power_gen as (
    select * from {{ ref('actual_power_generation') }}
),

final as (
    select
        year(start_date) as year,
        month(start_date) as month,
        sum(biomass_MWh) as biomass_MWh,
        sum(waterpower_MWh) as waterpower_MWh,
        sum(wind_offshore_MWh) as wind_offshore_MWh,
        sum(wind_onshore_MWh) as wind_onshore_MWh,
        sum(solar_MWh) as solar_MWh,
        sum(misc_renewables_MWh) as misc_renewables_MWh,
        sum(nuclear_MWh) as nuclear_MWh,
        sum(brown_coal_MWh) as brown_coal_MWh,
        sum(hard_coal_MWh) as hard_coal_MWh,
        sum(gas_MWh) as gas_MWh,
        sum(pump_storage_MWh) as pump_storage_MWh,
        sum(misc_conventional_MWh) as misc_conventional_MWh
    from power_gen
    group by 1, 2
)

select * from final
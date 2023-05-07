with actual_power_gen as (
    select * from {{ ref('actual_power_generation') }}
),

actual_power_gen_hourly as (
    select
        year(start_date) as year,
        month(start_date) as month,
        day(start_date) as day,
        hour(start_date) as hour,
        avg(biomass_MWh) as avg_biomass_MWh,
        avg(waterpower_MWh) as avg_waterpower_MWh,
        avg(wind_offshore_MWh) as avg_wind_offshore_MWh,
        avg(wind_onshore_MWh) as avg_wind_onshore_MWh,
        avg(solar_MWh) as avg_solar_MWh,
        avg(misc_renewables_MWh) as avg_misc_renewables_MWh,
        avg(nuclear_MWh) as avg_nuclear_MWh,
        avg(brown_coal_MWh) as avg_brown_coal_MWh,
        avg(hard_coal_MWh) as avg_hard_coal_MWh,
        avg(gas_MWh) as avg_gas_MWh,
        avg(pump_storage_MWh) as avg_pump_storage_MWh,
        avg(misc_conventional_MWh) as avg_misc_conventional_MWh
    from actual_power_gen
    group by 1, 2, 3, 4
),

final as (
    select
        year,
        sum(avg_biomass_MWh) as biomass_MWh,
        sum(avg_waterpower_MWh) as waterpower_MWh,
        sum(avg_wind_offshore_MWh) as wind_offshore_MWh,
        sum(avg_wind_onshore_MWh) as wind_onshore_MWh,
        sum(avg_solar_MWh) as solar_MWh,
        sum(avg_misc_renewables_MWh) as misc_renewables_MWh,
        sum(avg_nuclear_MWh) as nuclear_MWh,
        sum(avg_brown_coal_MWh) as brown_coal_MWh,
        sum(avg_hard_coal_MWh) as hard_coal_MWh,
        sum(avg_gas_MWh) as gas_MWh,
        sum(avg_pump_storage_MWh) as pump_storage_MWh,
        sum(avg_misc_conventional_MWh) as misc_conventional_MWh
    from actual_power_gen_hourly
    group by 1
)

select * from final
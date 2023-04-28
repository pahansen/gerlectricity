with installed_generation_c as (
    select * from {{ ref('installed_generation_capacity') }}
),

final as (
    select
        year(start_date) as year,
        -- growth rate of biomass_MW, waterpower_MW, wind_offshore_MW, wind_onshore_MW, solar_MW, misc_renewables_MW, nuclear_MW, brown_coal_MW, hard_coal_MW, gas_MW, pump_storage_MW, misc_conventional_MW   by previous year
        (biomass_MW - lag(biomass_MW, 1) over (order by start_date)) / lag(biomass_MW, 1) over (order by start_date) * 100 as biomass_MW_growth_rate,
        (waterpower_MW - lag(waterpower_MW, 1) over (order by start_date)) / lag(waterpower_MW, 1) over (order by start_date) * 100 as waterpower_MW_growth_rate,
        (wind_offshore_MW - lag(wind_offshore_MW, 1) over (order by start_date)) / lag(wind_offshore_MW, 1) over (order by start_date) * 100 as wind_offshore_MW_growth_rate,
        (wind_onshore_MW - lag(wind_onshore_MW, 1) over (order by start_date)) / lag(wind_onshore_MW, 1) over (order by start_date) * 100 as wind_onshore_MW_growth_rate,
        (solar_MW - lag(solar_MW, 1) over (order by start_date)) / lag(solar_MW, 1) over (order by start_date) * 100 as solar_MW_growth_rate,
        (misc_renewables_MW - lag(misc_renewables_MW, 1) over (order by start_date)) / lag(misc_renewables_MW, 1) over (order by start_date) * 100 as misc_renewables_MW_growth_rate,
        (nuclear_MW - lag(nuclear_MW, 1) over (order by start_date)) / lag(nuclear_MW, 1) over (order by start_date) * 100 as nuclear_MW_growth_rate,
        (brown_coal_MW - lag(brown_coal_MW, 1) over (order by start_date)) / lag(brown_coal_MW, 1) over (order by start_date) * 100 as brown_coal_MW_growth_rate,
        (hard_coal_MW - lag(hard_coal_MW, 1) over (order by start_date)) / lag(hard_coal_MW, 1) over (order by start_date) * 100 as hard_coal_MW_growth_rate,
        (gas_MW - lag(gas_MW, 1) over (order by start_date)) / lag(gas_MW, 1) over (order by start_date) * 100 as gas_MW_growth_rate,
        (pump_storage_MW - lag(pump_storage_MW, 1) over (order by start_date)) / lag(pump_storage_MW, 1) over (order by start_date) * 100 as pump_storage_MW_growth_rate,
        (misc_conventional_MW - lag(misc_conventional_MW, 1) over (order by start_date)) / lag(misc_conventional_MW, 1) over (order by start_date) * 100 as misc_conventional_MW_growth_rate
    from installed_generation_c
)

select * from final
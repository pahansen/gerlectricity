with installed_capacity as (
    select * from {{ ref('installed_generation_capacity') }}
),

actual_power_gen as (
    select * from {{ ref('actual_power_generation') }}
),

avg_power_gen_hourly as (
    select
        year(start_date) as year,
        month(start_date) as month,
        day(start_date) as day,
        hour(start_date) as hour,
        avg(biomass_MWh) as biomass_MWh,
        avg(waterpower_MWh) as waterpower_MWh,
        avg(wind_offshore_MWh) as wind_offshore_MWh,
        avg(wind_onshore_MWh) as wind_onshore_MWh,
        avg(solar_MWh) as solar_MWh,
        avg(misc_renewables_MWh) as misc_renewables_MWh,
        avg(nuclear_MWh) as nuclear_MWh,
        avg(brown_coal_MWh) as brown_coal_MWh,
        avg(hard_coal_MWh) as hard_coal_MWh,
        avg(gas_MWh) as gas_MWh,
        avg(pump_storage_MWh) as pump_storage_MWh,
        avg(misc_conventional_MWh) as misc_conventional_MWh
    from actual_power_gen
    group by year, month, day, hour
 ),

actual_power_gen_yearly as (
    select
        year,
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
    from avg_power_gen_hourly
    group by year
),

final as (
    select
        year(ic.start_date) as year,
        apg.biomass_MWh / (ic.biomass_MW * 8760)  as bio_utilization_MWh,
        apg.waterpower_MWh / (ic.waterpower_MW * 8760)  as water_utilization_MWh,
        apg.wind_offshore_MWh / (ic.wind_offshore_MW * 8760)  as wind_offshore_utilization_MWh,
        apg.wind_onshore_MWh / (ic.wind_onshore_MW * 8760)  as wind_onshore_utilization_MWh,
        apg.solar_MWh / (ic.solar_MW * 8760)  as solar_utilization_MWh,
        apg.misc_renewables_MWh / (ic.misc_renewables_MW * 8760)  as misc_renewables_utilization_MWh,
        apg.nuclear_MWh / (ic.nuclear_MW * 8760)  as nuclear_utilization_MWh,
        apg.brown_coal_MWh / (ic.brown_coal_MW * 8760)  as brown_coal_utilization_MWh,
        apg.hard_coal_MWh / (ic.hard_coal_MW * 8760)  as hard_coal_utilization_MWh,
        apg.gas_MWh / (ic.gas_MW * 8760)  as gas_utilization_MWh,
        apg.pump_storage_MWh / (ic.pump_storage_MW * 8760)  as pump_storage_utilization_MWh,
        apg.misc_conventional_MWh / (ic.misc_conventional_MW * 8760)  as misc_conventional_utilization_MWh
    from installed_capacity ic
    left join actual_power_gen_yearly apg on year(ic.start_date) = apg.year
)

select * from final

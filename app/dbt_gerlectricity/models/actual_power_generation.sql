with raw_actual_power as (
    select * from {{ source("main", "raw_actual_power_generation") }}
),

final as (
    select 
        strptime(concat(Date::varchar, ' ', "Start"), '%Y-%m-%d %H:%M') as start_date,
        strptime(concat(Date::varchar, ' ', "End"), '%Y-%m-%d %H:%M') as end_date,
        replace(replace("Biomasse [MWh] Calculated resolutions", '.', ''), ',', '.')::double as biomass_MWh,
        replace(replace("Wasserkraft [MWh] Calculated resolutions", '.', ''), ',', '.')::double as waterpower_MWh,
        replace(replace("Wind Offshore [MWh] Calculated resolutions", '.', ''), ',', '.')::double as wind_offshore_MWh,
        replace(replace("Wind Onshore [MWh] Calculated resolutions", '.', ''), ',', '.')::double as wind_onshore_MWh,
        replace(replace("Photovoltaik [MWh] Calculated resolutions", '.', ''), ',', '.')::double as solar_MWh,
        replace(replace("Sonstige Erneuerbare [MWh] Calculated resolutions", '.', ''), ',', '.')::double as misc_renewables_MWh,
        replace(replace("Kernenergie [MWh] Calculated resolutions", '.', ''), ',', '.')::double as nuclear_MWh,
        replace(replace("Braunkohle [MWh] Calculated resolutions", '.', ''), ',', '.')::double as brown_coal_MWh,
        replace(replace("Steinkohle [MWh] Calculated resolutions", '.', ''), ',', '.')::double as hard_coal_MWh,
        replace(replace("Erdgas [MWh] Calculated resolutions", '.', ''), ',', '.')::double as gas_MWh,
        replace(replace("Pumpspeicher [MWh] Calculated resolutions", '.', ''), ',', '.')::double as pump_storage_MWh,
        replace(replace("Sonstige Konventionelle [MWh] Calculated resolutions", '.', ''), ',', '.')::double as misc_conventional_MWh,
    from raw_actual_power_generation
)

select * from final
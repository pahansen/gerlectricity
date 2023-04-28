with installed_generation_c as (
    select * from {{ source('main', 'raw_installed_generation_capacity') }}
),

final as (
    select
        strptime(concat(Date::varchar, ' ', "Start"), '%Y-%m-%d %H:%M') as start_date,
        strptime(concat(Date::varchar, ' ', "End"), '%Y-%m-%d %H:%M') as end_date,
        replace(replace("Biomasse [MW] Original resolutions", '.', ''), ',', '.')::double as biomass_MW,
        replace(replace("Wasserkraft [MW] Original resolutions", '.', ''), ',', '.')::double as waterpower_MW,
        replace(replace("Wind Offshore [MW] Original resolutions", '.', ''), ',', '.')::double as wind_offshore_MW,
        replace(replace("Wind Onshore [MW] Original resolutions", '.', ''), ',', '.')::double as wind_onshore_MW,
        replace(replace("Photovoltaik [MW] Original resolutions", '.', ''), ',', '.')::double as solar_MW,
        replace(replace("Sonstige Erneuerbare [MW] Original resolutions", '.', ''), ',', '.')::double as misc_renewables_MW,
        replace(replace("Kernenergie [MW] Original resolutions", '.', ''), ',', '.')::double as nuclear_MW,
        replace(replace("Braunkohle [MW] Original resolutions", '.', ''), ',', '.')::double as brown_coal_MW,
        replace(replace("Steinkohle [MW] Original resolutions", '.', ''), ',', '.')::double as hard_coal_MW,
        replace(replace("Erdgas [MW] Original resolutions", '.', ''), ',', '.')::double as gas_MW,
        replace(replace("Pumpspeicher [MW] Original resolutions", '.', ''), ',', '.')::double as pump_storage_MW,
        replace(replace("Sonstige Konventionelle [MW] Original resolutions", '.', ''), ',', '.')::double as misc_conventional_MW
    from installed_generation_c
)

select * from final
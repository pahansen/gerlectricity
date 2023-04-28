with installed_generation_c as (
    select * from {{ source('main', 'raw_installed_generation_capacity') }}
),

final as (
    select
        strptime(concat(Date::varchar, ' ', "Start"), '%Y-%m-%d %H:%M') as start_date,
        strptime(concat(Date::varchar, ' ', "End"), '%Y-%m-%d %H:%M') as end_date,
        trim(replace(replace("Biomasse [MW] Original resolutions", '.', ''), ',', '.'))::double as biomass_MW,
        trim(replace(replace("Wasserkraft [MW] Original resolutions", '.', ''), ',', '.'))::double as waterpower_MW,
        trim(replace(replace("Wind Offshore [MW] Original resolutions", '.', ''), ',', '.'))::double as wind_offshore_MW,
        trim(replace(replace("Wind Onshore [MW] Original resolutions", '.', ''), ',', '.'))::double as wind_onshore_MW,
        trim(replace(replace("Photovoltaik [MW] Original resolutions", '.', ''), ',', '.'))::double as solar_MW,
        trim(replace(replace("Sonstige Erneuerbare [MW] Original resolutions", '.', ''), ',', '.'))::double as misc_renewables_MW,
        trim(replace(replace("Kernenergie [MW] Original resolutions", '.', ''), ',', '.'))::double as nuclear_MW,
        trim(replace(replace("Braunkohle [MW] Original resolutions", '.', ''), ',', '.'))::double as brown_coal_MW,
        trim(replace(replace("Steinkohle [MW] Original resolutions", '.', ''), ',', '.'))::double as hard_coal_MW,
        trim(replace(replace("Erdgas [MW] Original resolutions", '.', ''), ',', '.'))::double as gas_MW,
        trim(replace(replace("Pumpspeicher [MW] Original resolutions", '.', ''), ',', '.'))::double as pump_storage_MW,
        trim(replace(replace("Sonstige Konventionelle [MW] Original resolutions", '.', ''), ',', '.'))::double as misc_conventional_MW
    from installed_generation_c
)

select * from final

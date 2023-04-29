with installed_generation_c as (
    select * from {{ source('main', 'raw_installed_generation_capacity') }}
),

final as (
    select
        strptime(CONCAT(Date::varchar, ' ', "Start"), '%Y-%m-%d %H:%M') as start_date,
        strptime(CONCAT(Date::varchar, ' ', "End"), '%Y-%m-%d %H:%M') as end_date,
        "Biomasse [MW] Original resolutions"::decimal * 1000 as biomass_MW,
        "Wasserkraft [MW] Original resolutions"::decimal * 1000 as waterpower_MW,
        "Wind Offshore [MW] Original resolutions"::decimal * 1000 as wind_offshore_MW,
        "Wind Onshore [MW] Original resolutions"::decimal * 1000 as wind_onshore_MW,
        "Photovoltaik [MW] Original resolutions"::decimal * 1000 as solar_MW,
        "Sonstige Erneuerbare [MW] Original resolutions"::decimal as misc_renewables_MW,
        "Kernenergie [MW] Original resolutions"::decimal * 1000 as nuclear_MW,
        "Braunkohle [MW] Original resolutions"::decimal * 1000 as brown_coal_MW,
        "Steinkohle [MW] Original resolutions"::decimal * 1000 as hard_coal_MW,
        "Erdgas [MW] Original resolutions"::decimal * 1000 as gas_MW,
        "Pumpspeicher [MW] Original resolutions"::decimal * 1000 as pump_storage_MW,
        "Sonstige Konventionelle [MW] Original resolutions"::decimal * 1000 as misc_conventional_MW
    from installed_generation_c
)

select * from final

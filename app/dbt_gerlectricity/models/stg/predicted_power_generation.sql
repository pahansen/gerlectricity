with raw_predicted_power_g as (
    select * from {{ source("main", "raw_predicted_power_generation") }}
),

final as (
    select 
        strptime(concat(Date::varchar, ' ', "Start"), '%Y-%m-%d %H:%M') as start_date,
        strptime(concat(Date::varchar, ' ', "End"), '%Y-%m-%d %H:%M') as end_date,
        try_cast(replace(replace("Gesamt [MWh] Original resolutions", '.', ''), ',', '.') as double) as total_MWh,
        try_cast(replace(replace("Photovoltaik und Wind [MWh] Calculated resolutions", '.', ''), ',', '.') as double) as solar_and_wind_MWh,
        try_cast(replace(replace("Wind Offshore [MWh] Calculated resolutions", '.', ''), ',', '.') as double) as wind_offshore_MWh,
        try_cast(replace(replace("Wind Onshore [MWh] Calculated resolutions", '.', ''), ',', '.') as double) as wind_onshore_MWh,
        try_cast(replace(replace("Photovoltaik [MWh] Calculated resolutions", '.', ''), ',', '.') as double) as solar_MWh,
        try_cast(replace(replace("Sonstige [MWh] Original resolutions", '.', ''), ',', '.') as double) as misc_MWh
    from raw_predicted_power_g
)

select * from final


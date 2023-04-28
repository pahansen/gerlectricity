with raw_predicted_power_g as (
    select * from {{ source("main", "raw_predicted_power_generation") }}
),

final as (
    select 
        strptime(concat(Date::varchar, ' ', "Start"), '%Y-%m-%d %H:%M') as start_date,
        strptime(concat(Date::varchar, ' ', "End"), '%Y-%m-%d %H:%M') as end_date,
        case
            when "Gesamt [MWh] Original resolutions" = '-' then null
            else replace(replace("Gesamt [MWh] Original resolutions", '.', ''), ',', '.')::double
        end as total_MWh,
        case
            when "Photovoltaik und Wind [MWh] Calculated resolutions" = '-' then null
            else replace(replace("Photovoltaik und Wind [MWh] Calculated resolutions", '.', ''), ',', '.')::double
        end as solar_and_wind_MWh,
        case
            when "Wind Offshore [MWh] Calculated resolutions" = '-' then null
            else replace(replace("Wind Offshore [MWh] Calculated resolutions", '.', ''), ',', '.')::double
        end as wind_offshore_MWh,
        case
            when "Wind Onshore [MWh] Calculated resolutions" = '-' then null
            else replace(replace("Wind Onshore [MWh] Calculated resolutions", '.', ''), ',', '.')::double
        end as wind_onshore_MWh,
        case
            when "Photovoltaik [MWh] Calculated resolutions" = '-' then null
            else replace(replace("Photovoltaik [MWh] Calculated resolutions", '.', ''), ',', '.')::double
        end as solar_MWh,
        case
            when "Sonstige [MWh] Original resolutions" = '-' then null
            else replace(replace("Sonstige [MWh] Original resolutions", '.', ''), ',', '.')::double
        end as misc_MWh
    from raw_predicted_power_g
)

select * from final

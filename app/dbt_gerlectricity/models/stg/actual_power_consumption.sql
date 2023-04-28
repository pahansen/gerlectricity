with raw_actual_power_c as (
    select * from {{ source("main", "raw_actual_power_consumption") }}
),

final as (
    select 
        strptime(concat(Date::varchar, ' ', "Start"), '%Y-%m-%d %H:%M') as start_date,
        strptime(concat(Date::varchar, ' ', "End"), '%Y-%m-%d %H:%M') as end_date,
        replace(replace("Gesamt (Netzlast) [MWh] Calculated resolutions", '.', ''), ',', '.')::double as total_network_load_MWh,
        replace(replace("Residuallast [MWh] Calculated resolutions", '.', ''), ',', '.')::double as residual_load_MWh,
        replace(replace("Pumpspeicher [MWh] Calculated resolutions", '.', ''), ',', '.')::double as pump_storage_MWh
    from raw_actual_power_c
)

select * from final
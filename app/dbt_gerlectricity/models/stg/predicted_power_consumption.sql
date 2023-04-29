with raw_predicted_power_c as (
    select * from {{ source('main', 'raw_predicted_power_consumption') }}
),

final as (
    select
        strptime(concat(Datum::varchar, ' ', "Anfang"), '%Y-%m-%d %H:%M') as start_date,
        strptime(concat(Datum::varchar, ' ', "Ende"), '%Y-%m-%d %H:%M') as end_date,
        try_cast(replace(replace("Gesamt (Netzlast) [MWh] Berechnete Auflösungen", '.', ''), ',', '.') as double) as total_network_load_MWh,
        try_cast(replace(replace("Residuallast [MWh] Berechnete Auflösungen", '.', ''), ',', '.') as double) as residual_load_MWh
    from raw_predicted_power_c
)

select * from final



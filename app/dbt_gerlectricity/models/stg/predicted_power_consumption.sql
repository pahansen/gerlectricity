with raw_predicted_power_c as (
    select * from {{ source('main', 'raw_predicted_power_consumption') }}
),

final as (
    select
        strptime(concat(Datum::varchar, ' ', "Anfang"), '%Y-%m-%d %H:%M') as start_date,
        strptime(concat(Datum::varchar, ' ', "Ende"), '%Y-%m-%d %H:%M') as end_date,
        case
            when "Gesamt (Netzlast) [MWh] Berechnete Auflösungen" = '-' then null
            else replace(replace("Gesamt (Netzlast) [MWh] Berechnete Auflösungen", '.', ''), ',', '.')::double
        end as total_network_load_MWh,
        case
            when "Residuallast [MWh] Berechnete Auflösungen" = '-' then null
            else replace(replace("Residuallast [MWh] Berechnete Auflösungen", '.', ''), ',', '.')::double
        end as residual_load_MWh
    from raw_predicted_power_c
)

select * from final



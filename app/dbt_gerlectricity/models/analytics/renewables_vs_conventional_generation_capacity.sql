with installed_generation_c as (
    select * from {{ ref('installed_generation_capacity') }}
),

renewables_vs_conventional as (
    select
        year(start_date) as year,
        biomass_MW + waterpower_MW + wind_offshore_MW + wind_onshore_MW + solar_MW as renewables_MW,
        nuclear_MW + brown_coal_MW + hard_coal_MW + gas_MW + pump_storage_MW + misc_conventional_MW as conventional_MW
    from installed_generation_c
),

final as (
    select
        year,
        renewables_MW,
        conventional_MW,
        renewables_MW / (renewables_MW + conventional_MW) as renewables_share,
        renewables_MW / lag(renewables_MW) over (order by year) * 100 as renewables_growth_rate,
        conventional_MW / lag(conventional_MW) over (order by year) * 100 as conventional_growth_rate
    from renewables_vs_conventional
)

select * from final
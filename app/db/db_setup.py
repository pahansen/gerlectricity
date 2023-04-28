import duckdb

con = duckdb.connect('/workspaces/gerlectricity/gerlectricity.db')
con.sql(
    """
        create or replace table raw_actual_power_consumption as 
        select * 
        from read_csv_auto('/workspaces/gerlectricity/smard_data/Realisierter_Stromverbrauch_202101010000_202303312359_Hour.csv')
    """
)
con.sql(
    """
        create or replace table raw_actual_power_generation as 
        select * 
        from read_csv_auto('/workspaces/gerlectricity/smard_data/Realisierte_Erzeugung_202101010000_202303312359_Hour.csv')
    """
)
con.sql(
    """
        create or replace table raw_predicted_power_consumption as 
        select * 
        from read_csv_auto('/workspaces/gerlectricity/smard_data/Prognostizierter_Stromverbrauch_202101010000_202303312359_Stunde.csv')
    """
)
con.sql(
    """
        create or replace table raw_predicted_power_generation as 
        select * 
        from read_csv_auto('/workspaces/gerlectricity/smard_data/Prognostizierte_Erzeugung_202101010000_202303312359_Hour.csv')
    """
)
con.sql(
    """
        create or replace table raw_installed_generation_capacity as 
        select * 
        from read_csv_auto('/workspaces/gerlectricity/smard_data/Installierte_Erzeugungsleistung_202101010000_202303312359_Year.csv')
    """
)


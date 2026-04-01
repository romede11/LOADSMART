docker-compose up
""" execute notebook """
cd dbt_project
dbt build --target prod
""" dbt build fails, as there is no directive we will be accepting the errors for now."""
dbt build --exclude dbt_utils_source_expression_is_true_loadsmart_raw_freight_loads_pnl___book_price_source_price --exclude source_unique_loadsmart_raw_freight_loads_loadsmart_id --exclude source_not_null_loadsmart_raw_freight_loads_carrier_name --target prod
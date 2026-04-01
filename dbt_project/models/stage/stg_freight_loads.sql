{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='loadsmart_id'
) }}

with source as (

    select
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY loadsmart_id 
            ORDER BY greatest(
                cast(quote_date as timestamp),
                cast(book_date as timestamp),
                cast(source_date as timestamp),
                cast(pickup_date as timestamp),
                cast(delivery_date as timestamp)
            ) desc nulls last
        ) AS rn
    from {{ source('loadsmart', 'raw_freight_loads') }} r

),

renamed as (

    select
        loadsmart_id,
        lane,
        cast(quote_date as timestamp) as quote_date,
        cast(book_date as timestamp) as book_date,
        cast(source_date as timestamp) as source_date,
        cast(pickup_date as timestamp) as pickup_date,
        cast(delivery_date as timestamp) as delivery_date,
        cast(book_price as numeric) as book_price,
        cast(source_price as numeric) as source_price,
        cast(pnl as numeric) as pnl,
        cast(mileage as numeric) as mileage,
        equipment_type,
        cast(carrier_rating as numeric) as carrier_rating,
        sourcing_channel,
        vip_carrier,
        carrier_dropped_us_count,
        carrier_name,
        shipper_name,
        carrier_on_time_to_pickup,
        carrier_on_time_to_delivery,
        carrier_on_time_overall,
        cast(pickup_appointment_time as timestamp) as pickup_appointment_time,
        cast(delivery_appointment_time as timestamp) as delivery_appointment_time,
        has_mobile_app_tracking,
        has_macropoint_tracking,
        has_edi_tracking,
        contracted_load,
        load_booked_autonomously,
        load_sourced_autonomously,
        load_was_cancelled
    from source
    where rn = 1

),

control_columns as (

    select
        *,
            {{ dbt_utils.generate_surrogate_key([
            'lane',
            'quote_date',
            'book_date',
            'source_date',
            'pickup_date',
            'delivery_date',
            'book_price',
            'source_price',
            'pnl',
            'mileage',
            'equipment_type',
            'carrier_rating',
            'sourcing_channel',
            'vip_carrier',
            'carrier_dropped_us_count',
            'carrier_name',
            'shipper_name',
            'carrier_on_time_to_pickup',
            'carrier_on_time_to_delivery',
            'carrier_on_time_overall',
            'pickup_appointment_time',
            'delivery_appointment_time',
            'has_mobile_app_tracking',
            'has_macropoint_tracking',
            'has_edi_tracking',
            'contracted_load',
            'load_booked_autonomously',
            'load_sourced_autonomously',
            'load_was_cancelled'
            ]) }} as check_col_key
    from renamed

),

final as (

    select
        loadsmart_id,
        lane,
        quote_date,
        book_date,
        source_date,
        pickup_date,
        delivery_date,
        book_price,
        source_price,
        pnl,
        mileage,
        equipment_type,
        carrier_rating,
        sourcing_channel,
        vip_carrier,
        carrier_dropped_us_count,
        carrier_name,
        shipper_name,
        carrier_on_time_to_pickup,
        carrier_on_time_to_delivery,
        carrier_on_time_overall,
        pickup_appointment_time,
        delivery_appointment_time,
        has_mobile_app_tracking,
        has_macropoint_tracking,
        has_edi_tracking,
        contracted_load,
        load_booked_autonomously,
        load_sourced_autonomously,
        load_was_cancelled,
        check_col_key,
        cast(current_date as date) as dbt_created_at,
        cast(null as date) as dbt_updated_at
    from control_columns
{% if is_incremental() %}
    where loadsmart_id not in (select loadsmart_id from {{ this }} where loadsmart_id is not null)
union all
    select
        control_columns.loadsmart_id,
        control_columns.lane,
        control_columns.quote_date,
        control_columns.book_date,
        control_columns.source_date,
        control_columns.pickup_date,
        control_columns.delivery_date,
        control_columns.book_price,
        control_columns.source_price,
        control_columns.pnl,
        control_columns.mileage,
        control_columns.equipment_type,
        control_columns.carrier_rating,
        control_columns.sourcing_channel,
        control_columns.vip_carrier,
        control_columns.carrier_dropped_us_count,
        control_columns.carrier_name,
        control_columns.shipper_name,
        control_columns.carrier_on_time_to_pickup,
        control_columns.carrier_on_time_to_delivery,
        control_columns.carrier_on_time_overall,
        control_columns.pickup_appointment_time,
        control_columns.delivery_appointment_time,
        control_columns.has_mobile_app_tracking,
        control_columns.has_macropoint_tracking,
        control_columns.has_edi_tracking,
        control_columns.contracted_load,
        control_columns.load_booked_autonomously,
        control_columns.load_sourced_autonomously,
        control_columns.load_was_cancelled,
        control_columns.check_col_key,
        t.dbt_created_at,
        cast(current_date as date) as dbt_updated_at
    from control_columns
        inner join {{ this }} t
            on control_columns.loadsmart_id = t.loadsmart_id
            and control_columns.check_col_key != t.check_col_key
{% endif %}

)

select *
from final

with base as (

    select * from {{ ref('int_loads_enriched') }}

)

select
    loadsmart_id,

    -- foreign keys
    lane as location_id,
    carrier_name as carrier_id,

    -- dimensions
    upper(shipper_name) as shipper_id,

    -- dates
    quote_date,
    book_date,
    pickup_date,
    delivery_date,
    source_date,

    -- metrics
    carrier_rating,
    book_price,
    source_price,
    pnl,
    mileage,

    -- flags
    is_delivered,
    load_was_cancelled,
    contracted_load,

    -- performance
    carrier_on_time_to_pickup,
    carrier_on_time_to_delivery,
    carrier_on_time_overall,

    -- tracking
    -- has_mobile_app_tracking, -- same value always to be reactivated if this changes
    -- has_macropoint_tracking, -- same value always to be reactivated if this changes
    -- has_edi_tracking, -- same value always to be reactivated if this changes

    -- operational
    equipment_type,
    sourcing_channel

from base

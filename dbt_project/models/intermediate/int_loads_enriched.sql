with base as (

    select * from {{ ref('stg_loadsmart_tb') }}

),

split_lane as (

    select
        *,

        split_part(lane, '->', 1) as pickup_part,
        split_part(lane, '->', 2) as delivery_part

    from base
),

parsed as (

    select
        *,

        trim(split_part(pickup_part, ',', 1)) as pickup_city,
        trim(split_part(pickup_part, ',', 2)) as pickup_state,

        trim(split_part(delivery_part, ',', 1)) as delivery_city,
        trim(split_part(delivery_part, ',', 2)) as delivery_state,

        case 
            when delivery_date is not null then true 
            else false 
        end as is_delivered,

        date_trunc('month', delivery_date) as delivery_month

    from split_lane
)

select * from parsed

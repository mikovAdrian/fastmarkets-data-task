with source as (
    select * from {{ source('raw', 'orders_raw') }}
),
typed as (
    select
        trim(order_id)                           as order_id,
        trim(customer_id)                        as customer_id,
        -- try_to_date returns null instead of failing the whole build on one
        -- bad value. A hard cast would make a single malformed date take down
        -- every downstream model; a null is visible to the not_null test and
        -- leaves the rest of the run intact.
        try_to_date(trim(order_date))            as order_date,
        try_to_decimal(trim(order_total), 12, 2) as order_total,

        order_items,
        _source_row_hash,
        _loaded_at

    from source
)
select * from typed

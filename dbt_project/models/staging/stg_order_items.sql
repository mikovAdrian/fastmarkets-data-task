with orders as (
    select
        order_id,
        order_items
    from {{ ref('stg_orders') }}
),
flattened as (
    select
        orders.order_id,

        -- Position of the line inside the order's array. This is half the
        -- grain: order_id alone is not unique here, and two identical lines
        -- on the same order would otherwise be indistinguishable.
        item.index                            as line_number,

        trim(item.value:product_id::string)   as product_id,
        trim(item.value:product_name::string) as product_name,
        item.value:quantity::integer          as quantity,
        item.value:price::decimal(12, 2)      as unit_price
    from orders,
         lateral flatten(input => orders.order_items) as item
)

select * from flattened
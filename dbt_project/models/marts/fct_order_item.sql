with items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select
        order_id,
        customer_id,
        order_date
    from {{ ref('stg_orders') }}

),

final as (

    select
        -- The source gives no line identifier, so the key is generated over
        -- the natural grain. A fact table needs a single-column primary key:
        -- the uniqueness test can then be `unique` rather than a composite
        -- check, and anything joining to a line has one column to join on.
        {{ dbt_utils.generate_surrogate_key(['items.order_id', 'items.line_number']) }}
            as order_item_id,

        items.order_id,
        orders.customer_id,
        orders.order_date,

        -- The week definition lives here and only here. Everything that
        -- groups by week - agg_weekly_product_sales included - reads this
        -- column rather than repeating date_trunc, so there is one answer to
        -- "when does a week start" in the project.
        date_trunc('week', orders.order_date)::date as week_start,

        items.line_number,
        items.product_id,
        items.product_name,
        items.quantity,
        items.unit_price,

        -- Defined once, at the grain it belongs to. Every downstream model
        -- sums this column instead of repeating the multiplication, so the
        -- definition of what a line is worth cannot drift.
        items.quantity * items.unit_price as line_total

    from items

    -- inner join is correct here, unlike in fct_order: a line item whose
    -- parent order does not exist is an orphan, and it has no order_date, so
    -- it cannot be placed in a week. The relationships test would catch it,
    -- but the join must not invent a null week to carry it through.
    inner join orders
        on items.order_id = orders.order_id

)

select * from final
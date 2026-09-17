with orders as (
    select * from {{ ref('stg_orders') }}
),

line_totals as (
    select
        order_id,
        count(*)        as line_item_count,
        sum(quantity)   as total_quantity,

        -- Reads fct_order_item rather than stg_order_items so that
        -- quantity * unit_price is written in exactly one place. A fact
        -- reading another fact is a dependency worth accepting here: the
        -- alternative repeats the multiplication, and the two copies would
        -- drift the moment a discount or tax term is added to one of them.
        sum(line_total) as computed_total
    from {{ ref('fct_order_item') }}
    group by order_id
),
final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,

        -- What the source says the order is worth.
        orders.order_total,

        -- What the line items actually add up to.
        line_totals.computed_total,
        line_totals.line_item_count,
        line_totals.total_quantity,

        -- Surfaced as a column, not only as a test, so an analyst querying
        -- this table can see it without reading the dbt project.

        -- coalesce is load-bearing: an order with no line items at all would
        -- give a null computed_total, and `null != order_total` evaluates to
        -- null rather than true - so the mismatch would be invisible in
        -- exactly the case that most deserves attention.
        coalesce(line_totals.computed_total, 0) != orders.order_total
            as has_total_mismatch,

        orders._source_row_hash,
        orders._loaded_at
    from orders

    -- left join, not inner: an order that arrived with an empty items array
    -- must still appear in this table. An inner join would silently drop it,
    -- and a missing order is a worse defect than an order with no lines.
    left join line_totals
        on orders.order_id = line_totals.order_id
)
select * from final

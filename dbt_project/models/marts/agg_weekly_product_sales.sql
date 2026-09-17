{{ config(materialized = 'view') }}

with items as (

    select * from {{ ref('fct_order_item') }}

),

weekly as (

    select
        week_start,
        product_id,

        -- max() rather than grouping by product_name: the grain is
        -- (week_start, product_id), and grouping by the name as well would
        -- split a product into two rows if the source ever sent the name
        -- inconsistently. That would break both the stated grain and the
        -- uniqueness test, for a cosmetic reason.
        max(product_name)        as product_name,

        sum(line_total)          as total_revenue,
        sum(quantity)            as total_quantity,
        count(*)                 as line_item_count,
        count(distinct order_id) as order_count

    from items
    group by week_start, product_id

),

ranked as (

    select
        weekly.*,

        -- RANK, not ROW_NUMBER. With ROW_NUMBER a genuine tie is broken
        -- arbitrarily and one of two equally top-selling products is silently
        -- dropped from the flag - the output still looks correct while being
        -- wrong. RANK gives both the value 1, so both are flagged.
        rank() over (
            partition by week_start
            order by total_revenue desc
        ) as revenue_rank

    from weekly

)

select
    week_start,
    product_id,
    product_name,
    total_revenue,
    total_quantity,
    line_item_count,
    order_count,
    revenue_rank,

    -- Revenue, not quantity. Revenue answers "which product made us the most
    -- money", which is the standard commercial-performance question; quantity
    -- answers "which product moved the most units", which matters more for
    -- inventory and logistics. total_quantity stays in this model so the
    -- quantity view is still queryable without a schema change.
    revenue_rank = 1 as is_top_seller

from ranked
-- A structural guard, not a data guard.
--
-- fct_order carries line_item_count, total_quantity and computed_total as
-- degenerate measures at header grain. Those numbers must agree with the line
-- grain they summarise: sum(line_item_count) must equal the row count of
-- fct_order_item, and the same for quantity and revenue.
--
-- As the models are written today this cannot fail - fct_order derives its
-- measures from fct_order_item. That is the point. It fails the moment someone
-- changes the grain of either fact, swaps the join, or drops the GROUP BY, and
-- those are exactly the refactors that would otherwise go unnoticed because
-- every column-level test would still pass.

with header as (

    select
        coalesce(sum(line_item_count), 0) as lines_claimed,
        coalesce(sum(total_quantity), 0)  as quantity_claimed,
        coalesce(sum(computed_total), 0)  as revenue_claimed
    from {{ ref('fct_order') }}

),

lines as (

    select
        count(*)        as lines_actual,
        sum(quantity)   as quantity_actual,
        sum(line_total) as revenue_actual
    from {{ ref('fct_order_item') }}

)

select
    header.lines_claimed,
    lines.lines_actual,
    header.quantity_claimed,
    lines.quantity_actual,
    header.revenue_claimed,
    lines.revenue_actual

from header
cross join lines
where header.lines_claimed    != lines.lines_actual
   or header.quantity_claimed != lines.quantity_actual
   or header.revenue_claimed  != lines.revenue_actual

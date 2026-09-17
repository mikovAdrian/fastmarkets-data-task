-- A data guard that can genuinely fail.
--
-- An order arriving with an empty order_items array survives the load and the
-- staging layer intact: fct_order keeps it, because dropping an order is worse
-- than keeping one with no lines. Downstream it is invisible - it contributes
-- nothing to fct_order_item and nothing to the weekly aggregate, so revenue
-- reporting silently loses whatever the header total claimed.
--
-- has_total_mismatch would flag it, but only at warn severity, because a
-- header-versus-lines disagreement has legitimate causes. An order with no
-- lines at all does not, so it is an error here.

select
    order_id,
    customer_id,
    order_date,
    order_total,
    line_item_count

from {{ ref('fct_order') }}
where line_item_count is null
   or line_item_count = 0

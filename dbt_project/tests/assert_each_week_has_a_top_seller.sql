-- A singular test: it fails if any week with sales has no product flagged as
-- top seller.
--
-- Nothing in the column-level tests can catch this. accepted_values proves
-- is_top_seller holds only true or false; not_null proves it is populated.
-- Neither notices a refactor that leaves the flag false for a whole week -
-- RANK swapped for DENSE_RANK with a wrong offset, or a partition clause that
-- loses a week. The output would still look structurally valid.
--
-- dbt convention: a singular test returns the rows that violate the rule, so
-- returning zero rows means it passes.

with weekly as (
    select
        week_start,
        count(*)                as product_count,
        count_if(is_top_seller) as top_seller_count
    from {{ ref('agg_weekly_product_sales') }}
    group by week_start
)
select
    week_start,
    product_count,
    top_seller_count
from weekly
where top_seller_count = 0
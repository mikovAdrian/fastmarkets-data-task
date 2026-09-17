with source as (
    select * from {{ source('raw', 'orders_raw') }}
),

cleaned as (
    select
        trim(customer_id)                as customer_id,
        trim(customer_name)              as customer_name,

        -- The source double-escapes this field, so values arrive wrapped in
        -- literal double-quote characters: "+44-1693623526" rather than
        -- +44-1693623526. Strip them before validating.
        replace(customer_phone, '"', '') as customer_phone,

        lower(trim(customer_email))      as customer_email,

        -- Only used to decide which row wins per customer.
        try_to_date(trim(order_date))    as order_date,
        _loaded_at
    from source
),

flagged as (
    select
        customer_id,
        customer_name,
        customer_phone,
        customer_email,

        -- Snowflake's REGEXP_LIKE anchors the pattern at both ends, so ^ and $
        -- would be redundant: a match embedded in surrounding text already
        -- returns false. Verified against the engine rather than assumed.
        regexp_like(customer_phone, '\\+[0-9]{1,3}-[0-9]{6,10}') as is_valid_phone,

        regexp_like(customer_email, '[a-z0-9._%+\\-]+@[a-z0-9.\\-]+\\.[a-z]{2,}')
            -- The regex is the rule. This list only exists for placeholders
            -- that are syntactically well-formed, which a regex cannot detect.
            and customer_email not in ('n/a', 'na', 'none', 'unknown', 'invalid-email')
            as is_valid_email,
        order_date,
        _loaded_at
    from cleaned
),
deduplicated as (
    select *
    from flagged

    -- Collapse the one-big-table to a single row per customer, keeping the
    -- values from that customer's most recent order: contact details are more
    -- likely to be current on a recent order than an old one. _loaded_at
    -- breaks ties when one customer has two orders on the same date.
    -- This is SCD Type 1 - overwrite, no history.

    -- nulls last matters: Snowflake sorts nulls first on a DESC order, so
    -- without it a customer whose date failed to parse would win the dedup.
    qualify row_number() over (
        partition by customer_id
        order by order_date desc nulls last, _loaded_at desc
    ) = 1

)
select
    customer_id,
    customer_name,
    customer_phone,
    customer_email,
    is_valid_phone,
    is_valid_email
from deduplicated
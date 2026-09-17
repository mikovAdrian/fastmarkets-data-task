with customers as (
    select * from {{ ref('stg_customers') }}
),
final as (
    select
        -- customer_id is the primary key, used as-is rather than wrapped in asurrogate key. 
        -- A surrogate earns its place when a dimension carries
        -- history - SCD Type 2 gives one customer several rows, so the natural
        -- key stops being unique - or when ids from several source systems
        -- collide. Neither applies here: one source, SCD Type 1, one row per
        -- customer.
        customer_id,
        customer_name,
        customer_phone,
        customer_email,
        is_valid_phone,
        is_valid_email,

        -- Whether this customer is reachable at all. The two flags above say
        -- which channel is broken; this says whether anybody could be
        -- contacted, which is the question a campaign or a credit-control
        -- process actually asks.
        (is_valid_phone or is_valid_email) as has_valid_contact
    from customers
)
select * from final
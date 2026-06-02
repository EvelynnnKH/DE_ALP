with industries as (
    select distinct industry
    from {{ ref('stg_stock_prices') }}
),

final as (
    select
        to_hex(md5(industry)) as industry_key,
        industry
    from industries
)

select * from final
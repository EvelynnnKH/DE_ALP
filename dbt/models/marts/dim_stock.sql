with stocks as (
    select *
    from {{ ref('stg_stock_prices') }}
),

final as (
    select distinct
        to_hex(md5(ticker)) as stock_key,
        ticker,
        company_name
    from stocks
)

select * from final
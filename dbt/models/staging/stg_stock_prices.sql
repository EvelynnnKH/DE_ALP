with source as (

    select *
    from {{ source('raw', 'raw_stock_prices') }}

),

cleaned as (

    select

        date(date) as trade_date,

        upper(trim(ticker)) as ticker,

        trim(company_name) as company_name,
        trim(sector) as sector,
        trim(industry) as industry,

        safe_cast(open as numeric) as open_price,
        safe_cast(high as numeric) as high_price,
        safe_cast(low as numeric) as low_price,
        safe_cast(close as numeric) as close_price,
        safe_cast(adj_close as numeric) as adj_close_price,

        safe_cast(volume as int64) as volume

    from source

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by trade_date, ticker
            order by volume desc
        ) as row_num
    from cleaned

)

select
    trade_date,
    ticker,
    company_name,
    sector,
    industry,
    open_price,
    high_price,
    low_price,
    close_price,
    adj_close_price,
    volume

from deduplicated
where row_num = 1
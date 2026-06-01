{{
config(
    materialized='table'
)
}}

with stock_prices as (

    select *
    from {{ ref('stg_stock_prices') }}

),

stocks as (

    select *
    from {{ ref('dim_stock') }}

),

sectors as (

    select *
    from {{ ref('dim_sector') }}

),

industries as (

    select *
    from {{ ref('dim_industry') }}

),

dates as (

    select *
    from {{ ref('dim_date') }}

),

final as (

    select

        concat(
            cast(sp.trade_date as string),
            '_',
            sp.ticker
        ) as fact_id,

        d.date_key,

        s.stock_key,

        sec.sector_key,

        ind.industry_key,

        sp.open_price,
        sp.high_price,
        sp.low_price,
        sp.close_price,
        sp.adj_close_price,

        sp.volume,

        sp.close_price - sp.open_price
            as price_change,

        round(
            ((sp.close_price - sp.open_price)
            / nullif(sp.open_price,0))*100,
            2
        ) as daily_return_pct

    from stock_prices sp

    left join stocks s
        on sp.ticker = s.ticker

    left join sectors sec
        on sp.sector = sec.sector

    left join industries ind
        on sp.industry = ind.industry

    left join dates d
        on sp.trade_date = d.full_date

)

select *
from final

{% if is_incremental() %}

where date_key >= (
    select max(date_key)
    from {{ this }}
)

{% endif %}
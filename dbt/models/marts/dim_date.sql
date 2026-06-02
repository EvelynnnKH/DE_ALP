with dates as (

    select distinct
        trade_date as full_date

    from {{ ref('stg_stock_prices') }}

),

final as (

    select

        cast(format_date('%Y%m%d', full_date) as int64) as date_key,

        full_date,

        extract(year from full_date) as year,

        extract(month from full_date) as month_number,

        extract(day from full_date) as day_number,

        extract(quarter from full_date) as quarter_number,

        format_date('%Y-%m', full_date) as year_month

    from dates

)

select *
from final
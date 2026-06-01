with industries as (

    select distinct industry
    from {{ ref('stg_stock_prices') }}

),

final as (

    select

        row_number() over (
            order by industry
        ) as industry_key,

        industry

    from industries

)

select *
from final
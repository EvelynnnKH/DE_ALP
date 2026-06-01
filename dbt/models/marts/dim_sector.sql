with sectors as (

    select distinct sector
    from {{ ref('stg_stock_prices') }}

),

final as (

    select

        row_number() over (
            order by sector
        ) as sector_key,

        sector

    from sectors

)

select *
from final
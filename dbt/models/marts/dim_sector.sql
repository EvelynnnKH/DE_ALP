with sectors as (
    select distinct sector
    from {{ ref('stg_stock_prices') }}
),

final as (
    select
        to_hex(md5(sector)) as sector_key,
        sector
    from sectors
)

select * from final
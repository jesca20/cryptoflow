with latest as (
    select *,
        row_number() over (
            partition by id
            order by ingested_at desc
        ) as rn
    from {{ ref('stg_coins') }}
),

final as (
    select
        id,
        name,
        symbol_upper                as symbol,
        current_price,
        market_cap,
        total_volume,
        price_change_24h,
        ingested_at                 as last_seen_at,
        case
            when price_change_24h >= 5  then 'strong_gainer'
            when price_change_24h >= 0  then 'gainer'
            when price_change_24h >= -5 then 'loser'
            else 'strong_loser'
        end                         as movement_category
    from latest
    where rn = 1
)

select * from final
order by price_change_24h desc

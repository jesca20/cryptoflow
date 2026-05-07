
  create view "cryptoflow"."staging"."stg_coins__dbt_tmp"
    
    
  as (
    with source as (
    select * from raw.coins
),

deduped as (
    select *,
        row_number() over (
            partition by id, date_trunc('minute', ingested_at)
            order by ingested_at desc
        ) as rn
    from source
)

select
    id,
    symbol,
    upper(symbol)                    as symbol_upper,
    name,
    current_price::numeric(20, 8)    as current_price,
    market_cap,
    total_volume,
    price_change_24h::numeric(10, 4) as price_change_24h,
    ingested_at
from deduped
where rn = 1
  and current_price is not null
  and id is not null
  );
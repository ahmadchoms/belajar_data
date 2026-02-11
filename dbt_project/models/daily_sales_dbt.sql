{{ config(materialized='table') }}

-- extract & join
with raw_data as (
    select
        o.order_date,
        o.order_id,
        o.total_amount,
        p.category,
        o.status
    from {{ source('postgres_raw', 'orders') }} o
    join {{ source('postgres_raw', 'order_items') }} oi on o.order_id = oi.order_id
    join {{ source('postgres_raw', 'products') }} p on oi.product_id = p.product_id
    where o.status = 'Completed'
),

-- transform aggregation
daily_metriks as (
    select
        date(order_date) as report_date,
        sum(total_amount) as total_revenue,
        count(distinct order_id) as total_orders
    from raw_data
    group by date(order_date)
),

-- tranform category
category_ranking as (
    select
        date(order_date) as report_date,
        category,
        count(*) as items_sold,
        row_number() over (partition by date(order_date) order by count(*) desc) as rank
    from raw_data
    group by date(order_date), category
)

-- load
select
    m.report_date,
    m.total_revenue,
    m.total_orders,
    c.category as top_selling_category
from daily_metriks m
left join category_ranking c on m.report_date = c.report_date
where c.rank = 1 -- ambil hanya rank 1
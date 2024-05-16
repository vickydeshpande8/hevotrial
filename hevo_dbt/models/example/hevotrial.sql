{{ config(materialized='table') }}

with customers as (
    select id as c_id, 
            first_name, 
            last_name 
    from PC_HEVODATA_DB.PUBLIC.HEVO_TRIAL_RAW_CUSTOMERS
),
payments as (
    select * from PC_HEVODATA_DB.PUBLIC.HEVO_TRIAL_RAW_PAYMENTS
),
orders as (
    select id as o_id, 
            user_id, 
            order_date, 
            status 
    from PC_HEVODATA_DB.PUBLIC.HEVO_TRIAL_RAW_ORDERS
),
consolidated as (
    select * from (
        select * from customers c 
                        left join 
                    orders o 
                        on c.c_id = o.user_id
    ) h 
        left join 
    payments p 
        on h.o_id = p.order_id
),
clv_view as (
    select c_id, 
            sum(amount) as clv 
    from consolidated 
    group by c_id
),
num_orders_view as (
    select c.c_id as c_id, count(*) as num_orders 
    from customers c 
            left join 
        orders o 
    on c.c_id = o.user_id
    group by c_id
),
first_order_view as (
    select c_id, order_date as first_order_date
        from (
                select 
                    c.c_id as c_id, 
                    o.order_date as order_date,
                    ROW_NUMBER() OVER (
                                    PARTITION BY c_id 
                                    ORDER BY order_date
                                    ) as RowNumber
                from customers c 
                            left join 
                    orders o
            ) as l
        where RowNumber = 1
    ),
recent_order_view as (
    select c_id, order_date as recent_order_date
        from (
                select 
                    c.c_id as c_id, 
                    o.order_date as order_date,
                    ROW_NUMBER() OVER (
                                    PARTITION BY c_id 
                                    ORDER BY order_date DESC
                                    ) as RowNumber
                from customers c 
                            left join 
                    orders o
            ) as l
        where RowNumber = 1
    )

select v3.c_id as c_id, v3.clv as clv, v3.num_orders as num_orders, v3.recent_order_date as recent_order_date, v3.first_order_date as first_order_date, c.first_name as first_name, c.last_name as last_name from (
    select v2.c_id as c_id, v2.clv as clv, v2.num_orders as num_orders, v2.recent_order_date as recent_order_date, t4.first_order_date as first_order_date from(
        select v1.c_id as c_id, v1.clv as clv, v1.num_orders as num_orders, t3.recent_order_date as recent_order_date from (
            select t1.c_id as c_id, t1.clv as clv, t2.num_orders as num_orders from 
                clv_view t1
                    inner join 
                num_orders_view t2
                on t1.c_id = t2.c_id
            ) v1 
                inner join 
            recent_order_view t3
            on v1.c_id = t3.c_id
        ) v2 
            inner join 
        first_order_view t4
        on v2.c_id = t4.c_id
    ) v3
left join 
customers c
on v3.c_id = c.c_id
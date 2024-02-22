--- код для заполнения f_sales
insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
select dc.date_id, item_id, customer_id, city_id, quantity,
CASE WHEN  uol.status = 'refunded' then (-1 * payment_amount) else payment_amount end as payment_amount
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';
---where uol.date_time::Date = '2024-02-20';
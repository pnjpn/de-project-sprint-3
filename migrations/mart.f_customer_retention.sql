--- код для этапа 2 mart.f_customer_retention
--- создаем таблицу для второго этапа проeкта
DROP TABLE IF EXISTS mart.f_customer_retention;
CREATE TABLE mart.f_customer_retention (
  period_name DATE,
  period_id INTEGER,
  new_customers_count INTEGER,
  returning_customers_count INTEGER,
  refunded_customer_count INTEGER,
  item_id INTEGER,
  new_customers_revenue NUMERIC,
  returning_customers_revenue NUMERIC,
  customers_refunded INTEGER
);
--- группируем понедельно заказы по покупателям
WITH customer_counts AS (
  SELECT
    customer_id, 
    date_trunc('week', date_time) AS period_week, 
    COUNT(DISTINCT uniq_id) AS uniq_id_count, --- количество заказов у покупателя
    COUNT(DISTINCT CASE WHEN status = 'refunded' THEN uniq_id END) AS refunded_count --- количество возвратов у покупателя
  FROM
    staging.user_order_log
  GROUP BY
    customer_id, date_trunc('week', date_time)
),
--- собираем таблицу с нужными метриками
weekly_summary AS (
  SELECT
    period_week, --- неделя 
    EXTRACT(WEEK FROM period_week) AS period_id, --- номер недели
    COUNT(DISTINCT CASE WHEN uniq_id_count = 1 THEN cc.customer_id END) AS new_customers_count, ---- к-во новых покупателей
    COUNT(DISTINCT CASE WHEN uniq_id_count > 1 THEN cc.customer_id END) AS returning_customers_count, --- к-во новых покупателей с заказами >1
    COUNT(DISTINCT CASE WHEN refunded_count > 0 THEN cc.customer_id END) AS refunded_customer_count, --- к-во покупателей с возвратами
    COUNT(uol.item_id) AS item_id, --- к-во наименований товаров
    SUM(CASE WHEN uniq_id_count = 1 THEN 
              CASE WHEN uol.status = 'refunded' THEN -1 * uol.payment_amount ELSE uol.payment_amount END
            ELSE 0 END) AS new_customers_revenue, --- сумма выручки новых покупателей
    SUM(CASE WHEN uniq_id_count > 1 THEN 
              CASE WHEN uol.status = 'refunded' THEN -1 * uol.payment_amount ELSE uol.payment_amount END
            ELSE 0 END) AS returning_customers_revenue,  --- сумма выручки вернувшихся покупателей
    COUNT(DISTINCT CASE WHEN status = 'refunded' THEN uol.uniq_id END) AS customers_refunded  --- количество возвратов
  FROM
    customer_counts cc
    JOIN staging.user_order_log uol ON cc.customer_id = uol.customer_id
      AND cc.period_week = date_trunc('week', uol.date_time)
  GROUP BY
    period_week
),
---- соберем итоговую таблицу
total as (SELECT
  period_week AS period_name, --- неделя 
  period_id, --- номер недели
  new_customers_count, ---- к-во новых покупателей
  returning_customers_count, --- к-во вернувшихся покупателей
  refunded_customer_count, -- к-во покупателей с возвратами
  item_id, --- к-во наименований товаров (по ТЗ тут "идентификатор категории товара", но т.к. таблица с группировкой по неделям, то к-во)
  new_customers_revenue, -- сумма выручки новых покупателей
  returning_customers_revenue, --- сумма выручки вернувшихся покупателей
  customers_refunded --- количество возвратов
FROM
  weekly_summary
ORDER BY
  period_week)
INSERT INTO mart.f_customer_retention (
  period_name,
  period_id,
  new_customers_count,
  returning_customers_count,
  refunded_customer_count,
  item_id,
  new_customers_revenue,
  returning_customers_revenue,
  customers_refunded
)
select period_name,
  period_id,
  new_customers_count,
  returning_customers_count,
  refunded_customer_count,
  item_id,
  new_customers_revenue,
  returning_customers_revenue,
  customers_refunded
 from 
  total;
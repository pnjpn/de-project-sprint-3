--- код для добавим колонку статус в таблицу staging.user_order_log
--- status.user_order_log
ALTER TABLE staging.user_order_log
ADD status VARCHAR(100) NOT NULL;
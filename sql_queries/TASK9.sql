ALTER TABLE dim_orders
    ADD CONSTRAINT fk_orders_date FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
    ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_uuid)  REFERENCES dim_users(user_uuid),
    ADD CONSTRAINT fk_orders_store FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
    ADD CONSTRAINT fk_orders_product FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
    ADD CONSTRAINT fk_orders_card FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

-- SELECT card_number from dim_orders
-- WHERE card_number = '4971858637664481' --recleaned the card data table in vscode, it had values like "??" in card_number column
	
-- -- SELECT * FROM 
-- -- information_schema.tables
-- -- WHERE table_schema = 'public'
-- -- SELECT * FROM dim_users
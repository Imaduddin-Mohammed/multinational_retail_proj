ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid); 
ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid);
ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);
ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);
ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);

-- DELETE FROM dim_store_details
-- WHERE store_code IS NULL;  -- there were 3 null values in this column which had to be dropped

-- DELETE FROM dim_users
-- WHERE user_uuid IS NULL -- 21 nulls in user_uuid column before adding the primary key dropping them.

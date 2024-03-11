
Milestone 3: Create the database schema 
Task 1: Casting columns to the correct data_types
ALTER TABLE dim_orders
    ALTER COLUMN date_uuid TYPE UUID USING (date_uuid::UUID),
    ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::UUID),
    ALTER COLUMN card_number TYPE VARCHAR(255) USING (card_number::VARCHAR(255)), 
    ALTER COLUMN store_code TYPE VARCHAR(255) USING (store_code::VARCHAR(255)), 
    ALTER COLUMN product_code TYPE VARCHAR(255) USING (product_code::VARCHAR(255)), 
    ALTER COLUMN product_quantity TYPE SMALLINT USING (product_quantity::SMALLINT);

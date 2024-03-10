-- Task 7:  Casting columns to the correct data_types
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(255) USING (card_number::VARCHAR(255)),  -- Step 1: Change data types of columns
    ALTER COLUMN expiry_date TYPE VARCHAR(255) USING (expiry_date::VARCHAR(255)),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING (date_payment_confirmed::DATE);
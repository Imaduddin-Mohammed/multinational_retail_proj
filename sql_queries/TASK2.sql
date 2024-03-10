-- Task 2: Casting columns to the correct data_types
ALTER TABLE dim_user
    ALTER COLUMN first_name TYPE VARCHAR(255) USING (first_name::VARCHAR(255)),
    ALTER COLUMN last_name TYPE VARCHAR(255) USING (last_name::VARCHAR(255)),
    ALTER COLUMN date_of_birth TYPE DATE USING (date_of_birth::DATE),
    ALTER COLUMN country_code TYPE VARCHAR(255) USING (country_code::VARCHAR(255)), 
    ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::UUID),
    ALTER COLUMN join_date TYPE DATE USING (join_date::DATE);
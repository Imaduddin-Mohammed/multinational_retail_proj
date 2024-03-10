-- Task 6:  Casting columns to the correct data_types
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(255) USING (month::VARCHAR(255)),  -- Step 1: Change data types of columns
    ALTER COLUMN year TYPE VARCHAR(255) USING (year::VARCHAR(255)),
    ALTER COLUMN day TYPE VARCHAR(255) USING (day::VARCHAR(255)),
    ALTER COLUMN time_period TYPE VARCHAR(255) USING (time_period::VARCHAR(255)),
    ALTER COLUMN date_uuid TYPE UUID USING (date_uuid::UUID);
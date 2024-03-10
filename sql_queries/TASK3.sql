-- Task 3:  Casting columns to the correct data_types
-- SELECT *
--     FROM dim_store_details
--     WHERE longitude = 'N/A';

UPDATE dim_store_details
    SET address = NULL
    WHERE address = 'N/A';

UPDATE dim_store_details
    SET longitude = NULL
    WHERE longitude = 'N/A';

UPDATE dim_store_details
    SET locality = NULL
    WHERE locality = 'N/A';

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING (longitude::FLOAT),
    ALTER COLUMN locality TYPE VARCHAR(255) USING (locality::VARCHAR(255)),
    ALTER COLUMN store_code TYPE VARCHAR(255) USING (store_code::VARCHAR(255)), 
    ALTER COLUMN staff_numbers TYPE SMALLINT USING (staff_numbers::SMALLINT),
    ALTER COLUMN opening_date TYPE DATE USING (opening_date::DATE),
    ALTER COLUMN store_type TYPE VARCHAR(255) USING (store_type::VARCHAR(255)),
    ALTER COLUMN latitude TYPE FLOAT USING (latitude::FLOAT),
    ALTER COLUMN country_code TYPE VARCHAR(255) USING (country_code::VARCHAR(255)), 
    ALTER COLUMN continent TYPE VARCHAR(255) USING (continent::VARCHAR(255));
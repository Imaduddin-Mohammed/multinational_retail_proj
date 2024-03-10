ALTER TABLE dim_products  -- Step 1: Rename 'removed' to 'still_available'
    RENAME COLUMN removed TO still_available;
ALTER TABLE dim_products  -- Step 2: Change data types of columns
    ALTER COLUMN "product_price_(gbp)" TYPE FLOAT USING ("product_price_(gbp)"::FLOAT);
ALTER TABLE dim_products  
    ALTER COLUMN weight_in_kg TYPE FLOAT USING weight_in_kg::FLOAT;
ALTER TABLE dim_products
    ALTER COLUMN "EAN" TYPE VARCHAR(255) USING ("EAN"::VARCHAR(255));
ALTER TABLE dim_products 
    ALTER COLUMN product_code TYPE VARCHAR(255) USING (product_code::VARCHAR(255));
ALTER TABLE dim_products
    ALTER COLUMN date_added TYPE DATE USING (date_added::DATE);
ALTER TABLE dim_products 
    ALTER COLUMN uuid TYPE UUID USING (uuid::UUID);
ALTER TABLE dim_products
    ALTER COLUMN weight_class TYPE VARCHAR(255) USING (weight_class::VARCHAR(255));

-- ALTER TABLE dim_products
--     ALTER COLUMN still_available TYPE boolean USING (still_available = 'Still_available'::text::boolean);
ALTER TABLE dim_products
    ALTER COLUMN still_available TYPE BOOLEAN USING
    CASE
        WHEN still_available ILIKE 'Still_available' THEN TRUE
        ELSE FALSE
    END;
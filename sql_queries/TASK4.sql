Task 4:  Altering table to take weight_class column
UPDATE dim_products  
    SET product_price = REPLACE(product_price, '£', '')  -- Step 1: Remove '£' character from 'product_price' values
    WHERE product_price LIKE '£%';

ALTER TABLE dim_products 
    RENAME COLUMN product_price TO "product_price_(gbp)";  -- Step 2: Rename 'product_price' to 'product_price_(gbp)''
    
ALTER TABLE dim_products 
    ADD COLUMN IF NOT EXISTS weight_class VARCHAR(50);  -- Step 3: Add a new column 'weight_class'

UPDATE dim_products  -- Step 4: Update weight_class based on weight ranges
    SET weight_class = 
        CASE 
            WHEN "weight_in_kg" >= 0 AND "weight_in_kg" < 5 THEN 'Light'
            WHEN "weight_in_kg" >= 5 AND "weight_in_kg" < 10 THEN 'Medium'
            WHEN "weight_in_kg" >= 10 AND "weight_in_kg" < 20 THEN 'Heavy'
            ELSE 'Very Heavy'
        END;
from dateutil.parser import parse
import numpy as np
import pandas as pd
import re



class DataCleaning:

    def clean_legacy_user_data(self, legacy_users):

        cleaned_legacy_users_df = legacy_users.copy()
        cleaned_legacy_users_df.isna().mean()*100
        cleaned_legacy_users_df.replace('NULL', float("NaN"), inplace= True)
        cleaned_legacy_users_df.dropna(axis=0, inplace= True)
        cleaned_legacy_users_df.first_name = cleaned_legacy_users_df.first_name.astype('string')
        cleaned_legacy_users_df.last_name = cleaned_legacy_users_df.last_name.astype('string')
        cleaned_legacy_users_df.join_date = pd.to_datetime(cleaned_legacy_users_df['join_date'],infer_datetime_format= True, errors= 'coerce')
        cleaned_legacy_users_df.date_of_birth = pd.to_datetime(cleaned_legacy_users_df['date_of_birth'], infer_datetime_format= True, errors= 'coerce')
        cleaned_legacy_users_df.country = cleaned_legacy_users_df.country.astype('string')
        countries_to_keep = ['United Kingdom', 'Germany', 'United States']
        cleaned_legacy_users_df.country = cleaned_legacy_users_df.country[cleaned_legacy_users_df['country'].isin(countries_to_keep)]
        cleaned_legacy_users_df.isna().mean()*100 #Still there are some null values in the join_date - 24% and date_of_birth column - 27%
        cleaned_legacy_users_df.dropna(subset=['date_of_birth', 'join_date'], inplace= True)
        return cleaned_legacy_users_df
    

    def clean_legacy_store_data(self, store_df):

        cleaned_store_df = store_df.copy()
        cleaned_store_df.drop(columns = ['lat'], inplace = True)
        #row 447 in the entire store_df contains incorrect value
        cleaned_store_df.drop([447], inplace = True)
        #converting the staff_numbers column to numeric
        #keeping only 2 continents
        continents_to_keep = ['Europe', 'America', 'eeEurope', 'eeAmerica']
        cleaned_store_df.continent = cleaned_store_df.continent[cleaned_store_df.continent.isin(continents_to_keep)]
        #using .replace to map correctly
        cleaned_store_df['continent'] = store_df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})
        store_type_to_keep = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        cleaned_store_df.store_type = store_df.store_type[store_df.store_type.isin(store_type_to_keep)]
        cleaned_store_df.opening_date = pd.to_datetime(store_df.opening_date, infer_datetime_format= True, errors= 'coerce')
        return cleaned_store_df


    def clean_orders_data(self, orders_table):

        cleaned_order_df = orders_table.copy()
        #dropping these 3 columns because they are containing a large number of incorrect values
        cleaned_order_df.drop(columns = ['first_name', 'last_name', '1'], inplace = True)
        null_cols_percent = cleaned_order_df.isna().mean()*100
        print(null_cols_percent)
        return cleaned_order_df
    

    def clean_card_data(self, card_data):
        
        extracted_card_df = card_data.copy()
        print(extracted_card_df.date_payment_confirmed.head())
        print(f"Value count is: \n{extracted_card_df.date_payment_confirmed.value_counts()}")
        #we mask out these NULLS for the entire dataframe
        Nulls = ['NULL']
        mask = extracted_card_df.date_payment_confirmed.isin(Nulls)
        subset_df = extracted_card_df[mask]
        subset_df
        #from above we can see that there are 11NULL values present in this column which looking upon closely are entirely NULL for each column in the entire dataframe
        extracted_card_df.dropna(subset=['date_payment_confirmed'], inplace=True)
        #This doesnt drop nulls because they are not recognised by pandas because they are string
        print(extracted_card_df.date_payment_confirmed.iloc[377])
        #we check if NULLS are dropped. But they arent
        #we have to manually handle these NULL values in the dataframe because they might be string or in a format pandas cannot recognise.
        extracted_card_df.replace('NULL', float("NaN"), inplace= True)
        extracted_card_df.replace('', float("NaN"), inplace= True)
        extracted_card_df.replace(' ', float("NaN"), inplace= True)
        print(extracted_card_df.date_payment_confirmed.iloc[377])
        #dropping rows with all NaN values
        extracted_card_df.dropna(axis = 0, how='all', inplace= True)
        extracted_card_df.card_provider.value_counts()
        #Removing the gibberish values from the dataframe and saving it in a filtered dataframe namely df.
        gibberish_values = ['OGJTXI6X1H', 'BU9U947ZGV', 'UA07L7EILH', 'XGZBYBYGUW', 'DLWF2HANZF', '1M38DYQTZV', 'JRPRLPIBZ2',  'DE488ORDXY', '5CJH7ABGDR', 'JCQMU8FN85', 'TS8A81WFXV', 'WJVMUO4QX6', 'NB71VBAHJE', '5MFWFBZRM9']
        cleaned_card_df = extracted_card_df[~extracted_card_df.card_provider.isin(gibberish_values)]
        date_format = '%m/%y'
        cleaned_card_df.expiry_date = pd.to_datetime(cleaned_card_df.expiry_date, format = date_format, errors= 'coerce')
        cleaned_card_df.expiry_date
        #applying parse because there are inconsistent date strings in this column, due to which format fails.
        cleaned_card_df.date_payment_confirmed = cleaned_card_df.date_payment_confirmed.apply(parse)
        return cleaned_card_df
    

    def clean_store_data(self, stores_df):
        cleaned_stores_df = stores_df.copy()
        cleaned_stores_df.head(12)
        cleaned_stores_df.isna().mean()*100
        cleaned_stores_df.continent.value_counts()
        cleaned_stores_df.replace('NULL', float("NaN"), inplace= True)
        cleaned_stores_df.dropna(axis = 0, how = 'all', inplace=True)
        cleaned_stores_df.continent.value_counts()
        cleaned_stores_df.value_counts()
        gibberish_values = ['QMAVR5H3LD', 'LU3E036ZD9', '5586JCLARW', 'GFJQ2AAEQ8', 'SLQBD982C0', 'XQ953VS0FG', '1WZB1TE1HL']
        filtered_store_df = cleaned_stores_df[~cleaned_stores_df.continent.isin(gibberish_values)]
        filtered_store_df.continent.value_counts()
        filtered_store_df.continent.replace('eeEurope', 'Europe', inplace= True)
        filtered_store_df.continent.replace('eeAmerica', 'America', inplace= True)
        filtered_store_df.store_type = filtered_store_df.store_type.astype('category')
        filtered_store_df.opening_date = pd.to_datetime(filtered_store_df.opening_date, infer_datetime_format=True, errors = 'coerce')
        filtered_store_df.staff_numbers.unique()
        # Remove non-digit characters
        filtered_store_df.staff_numbers = filtered_store_df.staff_numbers.str.replace(r'\D', '', regex=True)
        # # Convert column to numeric, coerce errors
        filtered_store_df.staff_numbers = pd.to_numeric(filtered_store_df.staff_numbers, errors='coerce')
        filtered_store_df.staff_numbers.nunique()
        filtered_store_df.at[431,'staff_numbers']= float('NaN')
        return filtered_store_df
        
    
    def convert_product_weights(self, products_df_filtered):
        def convert_weight(value):
            try:
                if 'x' in value:   
                    numeric_part1, units1, numeric_part2, units2 = re.match(r"([\d.]+)\s*([a-zA-Z]*)\s*x\s*([\d.])\s*([a-zA-Z]*)", value).groups()
                    if not units1:
                        units1 = units2
                        result = float(numeric_part1) * float(numeric_part2)
                        if units1.lower() in ['g', 'gram', 'grams']:
                            result /= 1000
                        elif units1.lower() in ['ml', 'milliliter', 'milliliters']:
                            result /= 1000
                        elif units1.lower() in ['kg', 'kilogram', 'kilograms']:
                            pass  # No conversion needed for kg
                        elif units1.lower() in ['oz', 'ounce', 'ounces']:
                            result *= 0.0283495
                        else:
                            # If units are not recognized, return NaN
                            return np.nan
                else:
                    numeric_part, units = re.match(r"([\d.]+)\s*([a-zA-Z]*)", value).groups()
                    result = float(numeric_part)
                    if units.lower() in ['g', 'gram', 'grams']:
                        result /= 1000
                    elif units.lower() in ['ml', 'milliliter', 'milliliters']:
                        result /= 1000
                    elif units.lower() in ['kg', 'kilogram', 'kilograms']:
                        pass  # No conversion needed for kg
                    elif units.lower() in ['oz', 'ounce', 'ounces']:
                        result *= 0.0283495
                    else:
                        # If units are not recognised, return NaN
                        return np.nan
                    return round(result, 3)
                
            except Exception as e:
                return np.nan
            
        products_df_filtered['weight'] = products_df_filtered['weight'].apply(convert_weight)
        converted_prod_weight_df = products_df_filtered.rename(columns={'weight': 'weight_in_kg'})
        return converted_prod_weight_df
    
    def clean_products_data(self, product_df):
        products_df = product_df.copy()
        products_df_filtered = products_df[products_df['product_price'].notnull()]
        # regular expression to filter out rows where 'category' contains numbers
        products_df_filtered = products_df_filtered[~products_df_filtered['category'].str.contains('\d')]
        products_df_filtered['category'] = products_df_filtered['category'].astype('category')
        # Convert the 'date_added' column to datetime format
        products_df_filtered['date_added'] = products_df_filtered['date_added'].apply(parse)
        products_df_filtered['date_added'] = products_df_filtered['date_added'].combine_first(pd.to_datetime(products_df_filtered['date_added'], errors='coerce', format='%Y %B %d'))
        # Correct the spelling in the column 'removed'
        products_df_filtered['removed'] = products_df_filtered['removed'].replace('Still_avaliable', 'Still_available')
        # Convert 'removed' to datatype 'category'
        products_df_filtered['removed'] = products_df_filtered['removed'].astype('category')
        return products_df_filtered
    
    def clean_date_times(self, date_times):
        date_times_filtered = date_times.copy()
        date_times_filtered.timestamp = pd.to_datetime(date_times_filtered.timestamp, errors = 'coerce' , infer_datetime_format= True)
        gibberish_vals = ['9P3C0WBWTU', 'W6FT760O2B', 'DOIR43VTCM', 'FA8KD82QH3', 'ZRH2YT3FR8', '03T414PVFI', 'FNPZFYI489', '67RMH5U2R6', 'J9VQLERJQO', '2VZEREEIKB', 'K9ZN06ZS1X', 'SAT4V9O2DL', 'EB8VJHYZLE', '22JSMNGJCU', '4FHLELF101', 'DGQAH7M1HQ', '3ZZ5UCZR5D', 'YULO5U0ZAM', 'LZLLPZ0ZUA', 'NF46JOZMTA', '9GN4VIO5A8', '1YMRDJNU2T', 'GYSATSCN88']
        cleaned_date_times = date_times_filtered[~date_times_filtered.month.isin(gibberish_vals)]
        cleaned_date_times.replace('NULL', float("NaN"), inplace= True)
        cleaned_date_times.dropna(inplace= True)
        cleaned_date_times.time_period = cleaned_date_times.time_period.astype('category')
        return cleaned_date_times







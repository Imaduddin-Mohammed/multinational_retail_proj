from dateutil.parser import parse
import pandas as pd
import re
import numpy as np



class DataCleaning:

    def clean_legacy_user_data(self, legacy_users):
        user_df = legacy_users.copy()
        user_df.dropna(inplace = True)
        user_df.drop_duplicates( inplace = True)
        user_df.first_name = user_df.first_name.astype('string')
        user_df.last_name = user_df.last_name.astype('string')
        user_df.join_date = pd.to_datetime(user_df['join_date'],infer_datetime_format= True, errors= 'coerce')
        user_df.date_of_birth = pd.to_datetime(user_df['date_of_birth'], infer_datetime_format= True, errors= 'coerce')
        user_df.country = user_df.country.astype('string')
        #removed nulls from countries and kept the mask of only these 3
        countries_to_keep = ['United Kingdom', 'Germany', 'United States']
        user_df.country = user_df.country[user_df['country'].isin(countries_to_keep)]
        return user_df
    

    def clean_legacy_store_data(self, store_df):
        store_data = store_df.copy()
        store_data.drop(columns = ['lat'], inplace = True)
        col_with_null_percent = store_data.isna().mean()*100
        print(col_with_null_percent)
        #row 447 in the entire store_df contains incorrect value
        store_data.drop([447], inplace = True)
        #converting the staff_numbers column to numeric
        store_data.staff_numbers = pd.to_numeric(store_data.staff_numbers, errors= 'ignore')
        #keeping only 2 continents
        continents_to_keep = ['Europe', 'America', 'eeEurope', 'eeAmerica']
        store_data.continent = store_data.continent[store_data.continent.isin(continents_to_keep)]
        #using .replace to map correctly
        store_data['continent'] = store_df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})
        store_type_to_keep = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        store_data.store_type = store_df.store_type[store_df.store_type.isin(store_type_to_keep)]
        store_data.opening_date = pd.to_datetime(store_df.opening_date, infer_datetime_format= True, errors= 'coerce')
        store_data.reset_index()
        print(store_df.head(5))
        return store_data


    def clean_orders_data(self, orders_table):
        order_data = orders_table.copy()
        #dropping these 4 columns because they are containing a large number of incorrect values
        order_data.drop(columns = ['level_0', 'first_name', 'last_name', '1'], inplace = True)
        null_cols_percent = order_data.isna().mean()*100
        print(null_cols_percent)
        return order_data
    

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
        df = extracted_card_df[~extracted_card_df.card_provider.isin(gibberish_values)]
        date_format = '%m/%y'
        df.expiry_date = pd.to_datetime(df.expiry_date, format = date_format, errors= 'coerce')
        df.expiry_date
        #applying parse because there are inconsistent date strings in this column, due to which format fails.
        df.date_payment_confirmed = df.date_payment_confirmed.apply(parse)
        return df
    

    def clean_store_data(self, stores_df):
        store_data = stores_df.copy()
        store_data.isna().mean()*100
        store_data.continent.value_counts()
        store_data.replace('NULL', float("NaN"), inplace= True)
        store_data.isna().mean()*100
        store_data.value_counts()
        #removing gibberish values from the dataframe
        gibberish_values = ['QMAVR5H3LD', 'LU3E036ZD9', '5586JCLARW', 'GFJQ2AAEQ8', 'SLQBD982C0', 'XQ953VS0FG', '1WZB1TE1HL']
        cleaned_store_data = store_data[~store_data.continent.isin(gibberish_values)]
        cleaned_store_data.drop('lat', axis = 1 , inplace= True)
        cleaned_store_data.continent.replace('eeEurope', 'Europe', inplace= True)
        cleaned_store_data.continent.replace('eeAmerica', 'America', inplace= True)
        cleaned_store_data.dropna(how = 'all', axis= 0, inplace= True)
        cleaned_store_data.opening_date = cleaned_store_data.opening_date.astype('datetime64[ns]')
        cleaned_store_data.continent = cleaned_store_data.continent.astype('category')
        cleaned_store_data.country_code = cleaned_store_data.country_code.astype('category')
        return cleaned_store_data #3 rows contain entire NULL values, 217,405 & 437.
    
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



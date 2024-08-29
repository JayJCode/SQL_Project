import pandas as pd
import os

def extract(sql_data, parquet_data):
    
    parquet_data = pd.read_parquet(parquet_data)
    merged_df = pd.merge(sql_data, parquet_data, left_index=True, right_index=True)
    
    return merged_df

merged_df = extract(grocery_sales, "extra_data.parquet")
#print(merged_df.head())

def transform(raw_data):
    
    # Filling missed values and setting to mean
    raw_data_filled = raw_data.fillna(raw_data.mean())
    
    # Adding Month collumn
    raw_data_filled['Month'] = raw_data_filled['Date'].dt.month
    
    # Meaningfull collumns
    mean_column = [
        "Store_ID",
        "Month",
        "Dept",
        "IsHoliday",
        "Weekly_Sales",
        "CPI",
        "Unemployment"
    ]
    
    # Separate columns above and filter for sales over 10k
    clean_data = raw_data_filled.loc[raw_data_filled['Weekly_Sales'] > 10000, mean_column]
    
    return clean_data
    
clean_data = transform(merged_df)
#print(clean_data.head())

def avg_monthly_sales(clean_data):
    
    # Group data by Month
    data_grouped= clean_data.groupby(by=["Month"], axis=0).mean()
    data_grouped.reset_index(inplace=True)
    
    # Adding new collumn - Avg_Sales
    data_grouped['Avg_Sales'] = round(data_grouped['Weekly_Sales'], 2)
    
    # Select only Month and Avg_Sales
    agg_data = data_grouped.loc[:, ['Month', 'Avg_Sales']]
    
    return agg_data

agg_data = avg_monthly_sales(clean_data)
#print(agg_data)

def load(clean_data, clean_data_path, agg_data, agg_data_path):
    
    clean_data.to_csv(clean_data_path, index=False)
    agg_data.to_csv(agg_data_path, index=False)

load(clean_data, "clean_data.csv", agg_data, "agg_data.csv")

def validation(path):
    
    assert os.path.exists(path) == True

validation("clean_data.csv")
validation("agg_data.csv")

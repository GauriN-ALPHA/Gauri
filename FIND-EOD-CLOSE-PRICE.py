
# author: GAURI ANIL NAGKIRTI
#  #Purpose: Automated Scripts to Identify anomalies and discrepancies in the data OF CLOSE PRICE
#  #Contact: guari@alphapluscapital.in


from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import os

# Connect to MongoDB
client = MongoClient('mongodb://192.168.1.168:27017/')
db = client['EOD_data']  # Replace with your database name
collection = db['eod_data_alpha']  # Replace with your collection name

# Define the output folder path where the CSV file will be saved
output_folder = r'D:\Alpha'

try:
    # Define start and end dates for the query
    start_date = datetime(1995, 9, 15)
    end_date = datetime(2024, 6, 28)
    
    # Query MongoDB for documents within the date range
    query = {'date': {'$gte': start_date, '$lte': end_date}}
    cursor = collection.find(query).sort([('symbol', 1), ('date', 1)])  # Sort by symbol and then by date ascending
    
    # Initialize an empty list to store all data
    all_data = []
    
    # Dictionary to store the last close price for each symbol
    last_data = {}
    
    # Iterate through documents and process data
    for document in cursor:
        symbol = document['symbol']
        current_date = document['date'].strftime('%d-%m-%Y')  # Format current date as dd-mm-yyyy
        current_close_price = document['close']
        
        # Check if we have a previous close price for this symbol
        if symbol in last_data:
            last_close_price = last_data[symbol]['close']
            last_date = last_data[symbol]['date'].strftime('%d-%m-%Y') if last_data[symbol]['date'] else None
            if last_close_price is not None and last_close_price != 0:
                percentage_change = ((current_close_price - last_close_price) / last_close_price) * 100
                # Only include the data if the percentage change is less than 20%
                if abs(percentage_change) < 20:
                    data = {
                        'Date': last_date,
                        'Symbol': symbol,
                        'Last Date Close Price': last_close_price,
                        'Current Date': current_date,
                        'Current Date Close Price': current_close_price,
                        'Percentage Change': percentage_change
                    }
                    all_data.append(data)
        
        # Update last data for the symbol
        last_data[symbol] = {
            'close': current_close_price,
            'date': document['date']
        }
    
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(all_data)
    
    # Debug print to check DataFrame creation
    if df.empty:
        print("No data with percentage change less than 20% found.")
    else:
        print("DataFrame with percentage change less than 20% created.")
        print(df)  # Print the entire DataFrame

        # Save the DataFrame to a CSV file
        csv_filename = os.path.join(output_folder, "eod_data_less_than_20.csv")
        df.to_csv(csv_filename, index=False)
        print(f"CSV file saved: {csv_filename}")

        # Print the DataFrame and its shape
        print("\nComplete DataFrame with Percentage Change < 20%:")
        print(df)
        print("\nShape of the DataFrame:", df.shape)

except Exception as e:
    print(f"Error: {e}")

finally:
    client.close()

import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set your Electricitymaps API key here
api_key = os.getenv('_ELECTRICITY_API_KEY')

# Define the API endpoint and headers
endpoint = "https://api.electricitymap.org/v3/power-breakdown/history"
headers = {
    "auth-token": api_key
}

# Define parameters without start and end times for full history
params = {
    'zone': 'EE',  # Estonia’s country code
    'granularity': 'hour' # Not neccessary actually
}

# Make the API request
response = requests.get(endpoint, headers=headers, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()

    # Extract "history" data
    if 'history' in data:
        history = data['history']
        
        # Extract timestamps
        timestamps = [entry['datetime'] for entry in history]
        
        # Create DataFrames for consumption and production
        consumption_data = [entry['powerConsumptionBreakdown'] for entry in history]
        production_data = [entry['powerProductionBreakdown'] for entry in history]
        
        # Add timestamps to each DataFrame
        df_consumption = pd.DataFrame(consumption_data)
        df_consumption['datetime'] = pd.to_datetime(timestamps)
        
        df_production = pd.DataFrame(production_data)
        df_production['datetime'] = pd.to_datetime(timestamps)
        
        # Print start and end times of the data range
        print("Consumption Data:")
        print(df_consumption.head())
        print(f"Data Range - Start: {df_consumption['datetime'].min()}, End: {df_consumption['datetime'].max()}")
        
        print("\nProduction Data:")
        print(df_production.head())
        print(f"Data Range - Start: {df_production['datetime'].min()}, End: {df_production['datetime'].max()}")
    else:
        print("No data available or unexpected response format.")
else:
    print(f"Request failed with status code {response.status_code}: {response.text}")

#pip install os
#pip install python-dotenv

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set our ENTSO-E API token here
api_token = os.getenv('_ENTSOE_SECURITY_TOKEN')

# Define the endpoint and headers
endpoint = "https://web-api.tp.entsoe.eu/api"
headers = {
    'Authorization': f'Bearer {api_token}'
}

# Define interval for the previous day only
yesterday = datetime.utcnow() - timedelta(days=1)
start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y%m%d%H%M')
end_date = (yesterday + timedelta(hours=23)).strftime('%Y%m%d%H%M')

# Country EIC codes mapping for flexibility
country_eic_codes = {
    'EE': '10Y1001A1001A39I',  # Estonia
    'FI': '10YFI-1--------U' #Finland
    # Add other countries as needed: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_areas
}

# Set our desired country code here
country_code = 'EE'  # Change to desired country code
eic_code = country_eic_codes.get(country_code)

# Set parameters for day-ahead prices using the general country code
params = {
    'securityToken': api_token,
    'documentType': 'A44',  # Document type for day-ahead prices (although day-ahead, we can still look into past)
    'in_Domain': eic_code,  
    'out_Domain': eic_code, 
    'periodStart': start_date,
    'periodEnd': end_date
}

# Make the request to ENTSO-E API
response = requests.get(endpoint, headers=headers, params=params)

# Check if the request was successful
if response.status_code == 200:
    try:
        root = ET.fromstring(response.content)

        # Extract namespace
        namespace = {'ns': root.tag.split('}')[0].strip('{')}

        # Parse TimeSeries data
        prices = []
        timestamps = []

        for time_series in root.findall(".//ns:TimeSeries", namespace):
            # Determine the start time from the Period element
            period_start = time_series.find(".//ns:Period/ns:timeInterval/ns:start", namespace).text
            start_datetime = datetime.fromisoformat(period_start.replace("Z", "+00:00"))

            for point in time_series.findall(".//ns:Point", namespace):
                position = int(point.find("ns:position", namespace).text)
                price = float(point.find("ns:price.amount", namespace).text)

                # Calculate the specific timestamp for each position
                timestamp = start_datetime + timedelta(hours=position)
                prices.append(price)
                timestamps.append(timestamp)

        # Create a DataFrame with parsed data
        df = pd.DataFrame({
            'datetime': timestamps,
            'price': prices,
            'country': country_code
        })

        # Generate a full range of hourly timestamps for the previous day 
        # This is neccessary because we have "missing gaps" in the raw data
        full_time_range = pd.date_range(
            start=yesterday.replace(hour=0, minute=0, second=0, microsecond=0),
            end=yesterday.replace(hour=23, minute=0, second=0, microsecond=0),
            freq='H',
            tz='UTC'
        )

        # Reindex the DataFrame to ensure every hour is present and fill missing hours with NaN
        df = df.set_index('datetime').reindex(full_time_range).rename_axis('datetime').reset_index()
        df['country'] = country_code

        # Forward fill the NaN values in the 'price' column 
        # ENTSO-E API provides no information about hours where price did not change compared to the previous hour
        df['price'] = df['price'].fillna(method='ffill')

        if not df.empty:
            print("Price Data:")
            print(df)
            print(f"\nData Range: Start = {df['datetime'].min()}, End = {df['datetime'].max()}")
        else:
            print("No data extracted for the previous day. Check the XML structure or the query parameters.")
    except Exception as e:
        print("Error parsing XML:", e)
else:
    print(f"Request failed with status code {response.status_code}: {response.text}")

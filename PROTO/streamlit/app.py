import streamlit as st
import duckdb
import pandas as pd
import geopandas as gpd
import folium
from streamlit_folium import st_folium
from branca.colormap import LinearColormap
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.ticker as mticker

# ---- Configuration ----
db_path = "/usr/app/data/combined_data.duckdb"
geojson_url = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"

# Green energy sources
GREEN_SOURCES = ["Solar", "Biomass", "Wind Onshore", "Wind Offshore", "Geothermal", "Hydro Pumped Storage", 
                 "Hydro Run-of-river and poundage", "Hydro Water Reservoir", "Marine", "Other renewable"]

# Mapping BZN codes to country names
BZN_TO_COUNTRY = {
    "EE": "Estonia", "LV": "Latvia", "LT": "Lithuania", "FI": "Finland",
    "SE1": "Sweden", "SE2": "Sweden", "SE3": "Sweden", "SE4": "Sweden", 
    "NO1": "Norway", "NO2": "Norway", "NO3": "Norway", "NO4": "Norway", "NO5": "Norway",
    "DK1": "Denmark", "DK2": "Denmark", "DE-LU": "Germany", "PL": "Poland"
}

# Weather options with formatting
WEATHER_OPTIONS = {
    "Temperature": "temperature",
    "Relative Humidity": "relative_humidity",
    "Wind Speed": "wind_speed",
    "Precipitation": "precipitation",
    "Solar Radiation": "solar_radiation",
    "Air Pressure": "sea_level_pressure"
}

# Weather units
WEATHER_UNITS = {
    "Temperature": "°C",
    "Relative Humidity": "%",
    "Wind Speed": "m/s",
    "Precipitation": "mm",
    "Solar Radiation": "W/m²",
    "Air Pressure": "mmHg"
}


# ---- Dashboard Title ----
st.markdown(
    """
    <h1 style="text-align: center; font-size: 32px;">
        <span style="color: #03942c; font-weight: bold;">Renewable</span>
        <span style="color: #056fa3; font-weight: bold;">Energy</span><br>
        <span style="font-size: 20px; font-weight: normal;">Energy Production and Consumption Dashboard</span>
    </h1>
    """,
    unsafe_allow_html=True,
)

# ---- Connect to DuckDB ----
try:
    conn = duckdb.connect(db_path, read_only=True)
except Exception as e:
    st.error(f"Failed to connect to database: {e}")
    st.stop()

# ---- Fetch Filters ----
try:
    countries_query = "SELECT DISTINCT bzn FROM consumption_cleaned"
    countries = sorted([row[0] for row in conn.execute(countries_query).fetchall()])

    # Query unique hourly datetimes
    datetime_query = """
        SELECT DISTINCT DATE_TRUNC('hour', datetime) AS hourly_datetime
        FROM consumption_cleaned
        ORDER BY hourly_datetime
    """
    available_datetimes = conn.execute(datetime_query).fetchdf()["hourly_datetime"].to_list()

    # Convert to Python datetime objects
    available_datetimes = [
        pd.Timestamp(ts).to_pydatetime() for ts in available_datetimes
    ]
except Exception as e:
    st.error(f"Failed to fetch available hourly datetimes: {e}")
    st.stop()

# ---- Sidebar Filters ----
st.sidebar.header("Filters")
selected_country = st.sidebar.selectbox("Select a Country/Region", countries)

# Use select_slider for range selection with two endpoints
if available_datetimes:
    date_range = st.sidebar.select_slider(
        "Select Date Range",
        options=available_datetimes,
        value=(min(available_datetimes), max(available_datetimes)),  # Default to the full range
        format_func=lambda x: x.strftime("%Y-%m-%d %H:%M")  # Format display
    )
else:
    st.error("No hourly data available.")
    st.stop()

# Access selected start and end times
start_datetime, end_datetime = date_range


# ---- User Selections ----
st.sidebar.header("Plot Configuration")
data_type = st.sidebar.radio("Select Data Type", ["Consumption", "Production"])

secondary_axis = st.sidebar.radio("Select Secondary Y-Axis", ["Price", "Weather"])
weather_param = None
if secondary_axis == "Weather":
    weather_param = st.sidebar.selectbox("Select Weather Parameter", list(WEATHER_OPTIONS.keys()))

# ---- Query Data ----
try:
    # Base query based on selected data type
    if data_type == "Consumption":
        query = f"""
            SELECT datetime, quantity
            FROM consumption_cleaned
            WHERE bzn = '{selected_country}'
              AND datetime BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            ORDER BY datetime
        """
    else:
        query = f"""
            SELECT datetime, quantity, produced_energy_type
            FROM production_cleaned
            WHERE bzn = '{selected_country}'
              AND datetime BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            ORDER BY datetime
        """
    
    data = conn.execute(query).fetchdf()
    
    time_diff = (data["datetime"].iloc[1] - data["datetime"].iloc[0]).total_seconds() / 3600  # Difference in hours
    if time_diff <= 0.25:  # If the interval is 15 minutes or less
        bar_width = 0.0095
    else:  # For hourly or larger intervals
        bar_width = 0.04

    # Fetch secondary axis data
    if secondary_axis == "Price":
        price_query = f"""
            SELECT datetime, price
            FROM price_cleaned
            WHERE bzn = '{selected_country}'
              AND datetime BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            ORDER BY datetime
        """
        price_data = conn.execute(price_query).fetchdf()
    elif secondary_axis == "Weather" and weather_param:
        weather_column = WEATHER_OPTIONS[weather_param]  # Get the column name for weather parameter
        weather_query = f"""
            SELECT date_time, {weather_column} AS weather_value
            FROM weather_cleaned
            WHERE bzn = '{selected_country}'
              AND date_time BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            ORDER BY date_time
        """
        weather_data = conn.execute(weather_query).fetchdf()

        # If the selected weather parameter is "Wind_speed", divide by 3.6
        if weather_param == "Wind Speed":
            weather_data["weather_value"] = weather_data["weather_value"] / 3.6
        
except Exception as e:
    st.error(f"Failed to fetch data: {e}")
    st.stop()

# ---- Data Preprocessing ----
# Drop any rows with missing values in datetime or quantity columns
data.dropna(subset=["datetime", "quantity"], inplace=True)

# Ensure both datetime and quantity have the same length
if len(data["datetime"]) != len(data["quantity"]):
    st.error("Mismatch between datetime and quantity columns. Please check the data.")
    st.stop()

# ---- Handle Stacking of Green and Non-Green Data (only for production) ----
if data_type == "Production":
    # Split data into green and non-green energy sources
    green_data = data[data["produced_energy_type"].isin(GREEN_SOURCES)]
    non_green_data = data[~data["produced_energy_type"].isin(GREEN_SOURCES)]

    # Aggregate by datetime for proper stacking
    green_data = green_data.groupby("datetime", as_index=False).sum()
    non_green_data = non_green_data.groupby("datetime", as_index=False).sum()

# ---- Plot the Data ----
if not data.empty:
    fig, ax1 = plt.subplots(figsize=(14, 10))

    # Plot consumption as bars
    
    # Determine the time interval and adjust bar width accordingly
    time_diff = (data["datetime"].iloc[1] - data["datetime"].iloc[0]).total_seconds() / 3600  # Difference in hours
    if time_diff <= 0.25:  # If the interval is 15 minutes or less
        bar_width = 0.0095
    else:  # For hourly or larger intervals
        bar_width = 0.04
        
    if data_type == "Consumption":
        ax1.bar(data["datetime"], data["quantity"], color="#056fa3", alpha=0.55, width=bar_width)  # Adjust width based on interval
        ax1.set_ylabel("Consumption (MWh)", fontsize = 16)
        ax1.set_title(f"Consumption in {selected_country}", fontsize = 20)  # Title with country and date range

    # Plot production as stacked bars
    elif data_type == "Production":
        
        time_diff = (non_green_data["datetime"].iloc[1] - non_green_data["datetime"].iloc[0]).total_seconds() / 3600  # Difference in hours
        if time_diff <= 0.25:  # If the interval is 15 minutes or less
            bar_width = 0.0095
        else:  # For hourly or larger intervals
            bar_width = 0.04
        
        # Plot non-green energy bars first (bottom part of the stack)
        ax1.bar(
            non_green_data["datetime"], non_green_data["quantity"], color="grey", alpha=0.7, width=bar_width, label="Other Energy"
        )
        
        time_diff = (green_data["datetime"].iloc[1] - green_data["datetime"].iloc[0]).total_seconds() / 3600  # Difference in hours
        if time_diff <= 0.25:  # If the interval is 15 minutes or less
            bar_width = 0.0095
        else:  # For hourly or larger intervals
            bar_width = 0.04
            
        # Plot green energy bars on top of the non-green energy bars
        ax1.bar(
            green_data["datetime"], green_data["quantity"], color="green", alpha=0.7, width=bar_width, bottom=non_green_data["quantity"], label="Renewable Energy"
        )

        ax1.set_ylabel("Production (MWh)", fontsize = 16)
        ax1.set_title(f"Production in {selected_country}", fontsize = 20)  # Title with country and date range

    # Plot secondary axis
    if secondary_axis == "Price" and not price_data.empty:
        ax2 = ax1.twinx()
        ax2.plot(price_data["datetime"], price_data["price"], color="orange", label="Price (€/MWh)", linewidth=3)
        ax2.set_ylabel("Price (€/MWh)", color="orange", fontsize = 16)
        ax2.tick_params(axis='y', labelsize=14)
    elif secondary_axis == "Weather" and weather_param and not weather_data.empty:
        ax2 = ax1.twinx()
        ax2.plot(
            weather_data["date_time"], weather_data["weather_value"], color="darkred", label=weather_param, linewidth=3
        )
        ax2.set_ylabel(f"{weather_param} ({WEATHER_UNITS[weather_param]})", color="darkred", fontsize = 16)  # Add units to ylabel
        ax2.tick_params(axis='y', labelsize=14)
        
    # Show legend for green and non-green energy
    ax1.legend(loc="upper left", fontsize = 14)

    ax1.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d\n%H:%M'))
    ax1.tick_params(axis='x', labelsize=14)
    ax1.tick_params(axis='y', labelsize=14)
    fig.autofmt_xdate()
    st.pyplot(fig)
else:
    st.warning("No data available for the selected filters.")


# ---- Date Slider Above Map ----
#st.header("Green Energy Proportion Map")

# ---- Query Unique Hourly Datetimes ----
try:
    datetime_query = """
        SELECT DISTINCT datetime
        FROM production_cleaned
        WHERE EXTRACT(MINUTE FROM datetime) = 0  -- Only include hourly data
        ORDER BY datetime
    """
    available_datetimes = conn.execute(datetime_query).fetchdf()["datetime"].to_list()

    # Convert to Python datetime objects
    available_datetimes = [
        pd.Timestamp(ts).to_pydatetime() for ts in available_datetimes
    ]
except Exception as e:
    st.error(f"Failed to fetch available hourly datetimes: {e}")
    st.stop()

# Adjust slider for hourly datetimes only
if available_datetimes:
    selected_datetime = st.select_slider(
        "",
        options=available_datetimes,
        value=min(available_datetimes),  # Default to the earliest time
    )
else:
    st.error("No hourly data available.")
    st.stop()

# ---- Query Energy Data ----
try:
    energy_query = f"""
        SELECT bzn, SUM(CASE WHEN produced_energy_type IN ({','.join([f"'{s}'" for s in GREEN_SOURCES])})
                    THEN quantity ELSE 0 END) / SUM(quantity) AS green_proportion
        FROM production_cleaned
        WHERE datetime = '{selected_datetime}'
        GROUP BY bzn
    """
    energy_data = conn.execute(energy_query).fetchdf()
    # Map BZN to country names
    energy_data["country"] = energy_data["bzn"].map(BZN_TO_COUNTRY)

    # Aggregate by country (average green proportion)
    country_data = (
        energy_data.groupby("country")["green_proportion"]
        .mean()
        .reset_index()
    )
    
    # Ensure all countries in BZN_TO_COUNTRY are represented
    all_countries = pd.DataFrame({"country": list(BZN_TO_COUNTRY.values())})
    country_data = all_countries.merge(
        country_data, on="country", how="left"
    ).fillna(0)  # Fill missing proportions with 0
except Exception as e:
    st.error(f"Failed to fetch energy data: {e}")
    st.stop()

# ---- Download and Load GeoJSON ----
try:
    geodata = gpd.read_file(geojson_url)
except Exception as e:
    st.error(f"Failed to load GeoJSON data: {e}")
    st.stop()

# ---- Merge GeoJSON with Energy Data ----
geodata = geodata.merge(country_data, left_on="name", right_on="country", how="left")

# ---- Create the Interactive Map ----
# Define a custom linear colormap
colormap = LinearColormap(
    colors=["#f3f5d7", "#addd8e", "#1c6304"],
    vmin=0,
    vmax=1,
    caption="Proportion of Renewable Energy"
)

# Create the map
m = folium.Map(location=[57.52, 16.5], zoom_start=4,
               tiles="https://basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png",
                attr="CartoDB Positron Lite (No Labels)")  # Centered roughly on Northern Europe

# Add GeoJSON with style based on green energy proportion
folium.GeoJson(
    geodata,
    style_function=lambda feature: {
        "fillColor": colormap(feature["properties"]["green_proportion"])
        if feature["properties"]["green_proportion"] is not None else "#dfe0de",
        "color": "black",
        "weight": 0.5,
        "fillOpacity": 0.9,
    },
    tooltip=folium.GeoJsonTooltip(
        fields=["name", "green_proportion"],
        aliases=["Country", "Renewable Energy Proportion"],
        localize=True,
    )
).add_to(m)

# Add the colormap to the map (legend moved to bottom left)
colormap.add_to(m)
colormap.caption_style = {"bottom": "10px", "left": "10px"}

# Display the map
st_folium(m, width=700, height=400)
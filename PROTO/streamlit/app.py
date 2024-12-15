import streamlit as st
import duckdb
import pandas as pd
import geopandas as gpd
import folium
from streamlit_folium import st_folium
from branca.colormap import LinearColormap
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

# ---- Configuration ----
db_path = "/usr/app/data/star_schema_db.duckdb"
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

st.set_page_config(layout="wide")

# ---- Dashboard Title ----
st.markdown(
    """
    <h1 style="text-align: center; font-size: 50px; margin-top: -45px;">
        <span style="color: #03942c; font-weight: bold;">Renewable</span>
        <span style="color: #056fa3; font-weight: bold;">Energy</span><br>
        <span style="font-size: 22px; font-weight: normal;">Production and Consumption in Northern Europe</span>
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
    countries_query = "SELECT DISTINCT bzn FROM measurements_FACT;"
    countries = sorted([row[0] for row in conn.execute(countries_query).fetchall()])

    # Query unique hourly datetimes
    datetime_query = """
        SELECT DISTINCT DATE_TRUNC('hour', datetime) AS hourly_datetime
        FROM measurements_FACT
        ORDER BY hourly_datetime;
    """
    
    available_datetimes = conn.execute(datetime_query).fetchdf()["hourly_datetime"].to_list()

    # Filter out None values before conversion
    available_datetimes = [
        pd.Timestamp(ts).to_pydatetime() for ts in available_datetimes if ts is not None
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
    # Dynamically fetch all production_quantity columns
    production_columns_query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'measurements_FACT'
          AND column_name LIKE 'production_quantity_%'
    """
    production_columns = [row[0] for row in conn.execute(production_columns_query).fetchall()]

    # Construct the query to fetch all required data
    query = f"""
        SELECT datetime, 
               consumption_quantity, 
               {', '.join(production_columns)},
               price, 
               {', '.join(WEATHER_OPTIONS.values())}
        FROM measurements_FACT
        WHERE bzn = '{selected_country}'
          AND datetime BETWEEN '{start_datetime}' AND '{end_datetime}' 
        ORDER BY datetime
    """
    data = conn.execute(query).fetchdf()
    
    data = data[data['consumption_quantity'] > 0] #Remove "empty" rows
    
    time_diff = (data["datetime"].iloc[1] - data["datetime"].iloc[0]).total_seconds() / 3600  # Difference in hours
    bar_width = 0.0095 if time_diff <= 0.25 else 0.04

except Exception as e:
    st.error(f"Failed to fetch data: {e}")
    st.stop()

# ---- Data Preprocessing ----
if data_type == "Production":
    # Identify green and non-green energy columns
    green_columns = [f"production_quantity_{source}" for source in [source.lower().replace(" ", "_") for source in GREEN_SOURCES] if f"production_quantity_{source}" in data.columns]
    non_green_columns = [col for col in production_columns if col not in green_columns]

    # Calculate sums for green and non-green energy
    data["green_energy"] = data[green_columns].sum(axis=1) if green_columns else 0
    data["non_green_energy"] = data[non_green_columns].sum(axis=1) if non_green_columns else 0

# ---- Plot the Data ----
if not data.empty:
    fig, ax1 = plt.subplots(figsize=(14, 6))

    if data_type == "Consumption":
        ax1.bar(data["datetime"], data["consumption_quantity"], color="#056fa3", alpha=0.55, width=bar_width)
        ax1.set_ylabel("Consumption (MWh)", fontsize=14)
        ax1.set_title(f"Energy Consumption in {selected_country}", fontsize=16)

    elif data_type == "Production":
        ax1.bar(
            data["datetime"], data["non_green_energy"],
            color="grey", alpha=0.7, width=bar_width, label="Non-Renewable Energy"
        )
        ax1.bar(
            data["datetime"], data["green_energy"],
            color="green", alpha=0.7, width=bar_width, bottom=data["non_green_energy"], label="Renewable Energy"
        )
        ax1.set_ylabel("Production (MWh)", fontsize=14)
        ax1.set_title(f"Energy Production in {selected_country}", fontsize=16)

    # Plot secondary axis (price or weather)
    if secondary_axis == "Price":
        ax2 = ax1.twinx()
        ax2.plot(data["datetime"], data["price"], color="orange", linewidth=3, label="Price (€/MWh)")
        ax2.set_ylabel("Price (€/MWh)", color="orange", fontsize=14)
    elif secondary_axis == "Weather" and weather_param:
        ax2 = ax1.twinx()
        ax2.plot(data["datetime"], data[WEATHER_OPTIONS[weather_param]], color="darkred", linewidth=3, label=weather_param)
        ax2.set_ylabel(f"{weather_param} ({WEATHER_UNITS[weather_param]})", color="darkred", fontsize=14)

    ax1.legend(loc="upper left", fontsize=12)
    ax1.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d\n%H:%M'))
    fig.autofmt_xdate()
    st.pyplot(fig)
else:
    st.warning("No data available for the selected filters.")



# ---- Query Unique Hourly Datetimes ----
try:
    datetime_query = """
        SELECT DISTINCT DATE_TRUNC('hour', datetime) AS hourly_datetime
        FROM measurements_FACT
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
    # Dynamically fetch all production_quantity columns
    production_columns_query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'measurements_FACT'
          AND column_name LIKE 'production_quantity_%'
    """
    production_columns = [row[0] for row in conn.execute(production_columns_query).fetchall()]

    # Create green and non-green mapping
    green_columns = [f"production_quantity_{source}" for source in [source.lower().replace(" ", "_") for source in GREEN_SOURCES] if f"production_quantity_{source}" in production_columns]
    non_green_columns = [col for col in production_columns if col not in green_columns]

    # Query to calculate green energy proportion for each country
    @st.cache_data
    def fetch_energy_data(selected_datetime):
        query = f"""
            SELECT bzn,
                SUM({'+'.join(green_columns)}) AS green_energy,
                SUM({'+'.join(non_green_columns)}) AS non_green_energy,
                SUM({'+'.join(production_columns)}) AS total_energy
            FROM measurements_FACT
            WHERE DATE_TRUNC('hour', datetime) = '{selected_datetime}'
            GROUP BY bzn
        """
        return conn.execute(query).fetchdf()

    energy_data = fetch_energy_data(selected_datetime)

    # Calculate green proportion
    energy_data["green_proportion"] = energy_data["green_energy"] / energy_data["total_energy"]

    # Map BZN to country names
    energy_data["country"] = energy_data["bzn"].map(BZN_TO_COUNTRY)

    # Ensure all countries in BZN_TO_COUNTRY are represented
    all_countries = pd.DataFrame({"country": list(BZN_TO_COUNTRY.values())})
    country_data = all_countries.merge(
        energy_data[["country", "green_proportion"]], on="country", how="left"
    ).fillna(0)  # Fill missing proportions with 0
except Exception as e:
    st.error(f"Failed to fetch energy data: {e}")
    st.stop()

# ---- Download and Load GeoJSON ----
try:
    geodata = gpd.read_file(geojson_url)
    geodata = geodata[geodata["name"].isin(BZN_TO_COUNTRY.values())]
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
m = folium.Map(location=[62, 16.5], zoom_start=3,
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

# Display the map
st_folium(m, width=950, height=400)

# Add the final text under the map
st.markdown(
    """
    <p style="font-size: 12px; font-style: italic; text-align: center; margin-top: -30px;">
        For the sake of plotting simplicity, the share of renewable energy in different regions of Norway, Sweden, and Denmark has been averaged.
    </p>
    """,
    unsafe_allow_html=True,
)
import streamlit as st
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.ticker as mticker

# ---- Dashboard Title ----
st.markdown(
    """
    <h1 style="text-align: center; font-size: 32px;">
        <span style="color: #03942c; font-weight: bold;">Renewable</span>
        <span style="color: #056fa3; font-weight: bold;">Energy</span><br>
        <span style="font-size: 20px; font-weight: normal;">Production and Consumption in Northern Europe</span>
    </h1>
    """,
    unsafe_allow_html=True,
)

# ---- Connect to DuckDB ----
db_path = "/usr/app/data/weather.duckdb"
try:
    weather = duckdb.connect(db_path, read_only=True)
except Exception as e:
    st.error(f"Failed to connect to database: {e}")
    st.stop()

# ---- Fetch Data for Filters ----
# Query for date range and distinct countries
try:
    date_query = """
        SELECT MIN(date_time) AS min_date, MAX(date_time) AS max_date
        FROM weather_cleaned
    """
    data_info = weather.execute(date_query).fetchone()
    min_date, max_date = pd.to_datetime(data_info[0]), pd.to_datetime(data_info[1])
    min_date, max_date = min_date.to_pydatetime(), max_date.to_pydatetime()

    distinct_countries_query = """
        SELECT DISTINCT country 
        FROM weather_cleaned
    """
    countries = sorted([row[0] for row in weather.execute(distinct_countries_query).fetchall()])  # Sort alphabetically
except Exception as e:
    st.error(f"Failed to fetch filter data: {e}")
    st.stop()

# ---- Sidebar Filters ----
st.sidebar.header("Filters")
selected_country = st.sidebar.selectbox("Select a Country", countries)
date_range = st.sidebar.slider(
    "Select Date Range",
    min_value=min_date,
    max_value=max_date,
    value=(min_date, max_date),
    format="YYYY-MM-DD HH:mm"
)

# ---- Filtered Data Query ----
filtered_query = f"""
    SELECT date_time, temperature, solar_radiation
    FROM weather_cleaned
    WHERE country = '{selected_country}'
      AND date_time BETWEEN '{date_range[0].strftime("%Y-%m-%d %H:%M:%S")}' AND '{date_range[1].strftime("%Y-%m-%d %H:%M:%S")}'
    ORDER BY date_time
"""
try:
    filtered_data = weather.execute(filtered_query).fetchdf()
except Exception as e:
    st.error(f"Failed to fetch filtered data: {e}")
    st.stop()

# ---- Plot the Data ----
if not filtered_data.empty:
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Convert date_time to datetime
    filtered_data["date_time"] = pd.to_datetime(filtered_data["date_time"])

    # Plot temperature with different colors based on regions
    for i in range(len(filtered_data) - 1):
        x = filtered_data["date_time"].iloc[i : i + 2]
        y = filtered_data["temperature"].iloc[i : i + 2]
        color = "red" if y.mean() >= 0 else "#07a4f2"
        ax1.plot(x, y, color=color, linewidth=2)

    # Set y-axis label for temperature
    ax1.set_ylabel("Temperature (°C)")
    ax1.tick_params(axis="y")  # Default black ticks for Y-axis

    # Ensure y-axis tick values are integers
    ax1.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))

    # Add second y-axis for solar radiation
    ax2 = ax1.twinx()
    ax2.plot(
        filtered_data["date_time"],
        filtered_data["solar_radiation"],
        label="Solar Radiation (W/m²)",
        color="orange",
        linewidth=2  # Thicker line
    )
    ax2.set_ylabel("Solar Radiation (W/m²)", color="orange")  # Set label color to orange
    ax2.tick_params(axis="y")  # Default black ticks for Y-axis

    # Format x-axis with hours and minutes in the first row
    ax1.xaxis.set_major_formatter(DateFormatter('%H:%M\n%Y-%m-%d'))

    # Remove x-axis label
    ax1.set_xlabel(None)

    # Adjust layout
    fig.tight_layout()
    st.pyplot(fig)
else:
    st.warning("No data available for the selected filters.")
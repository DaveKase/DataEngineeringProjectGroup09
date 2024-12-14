# WattWatchers

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Overview
As nations work towards ambitious climate goals, gaining a deeper understanding of renewable energy dynamics, weather patterns, and their economic implications is more important than ever. 
This project provides actionable insights through real-time visualizations and reports, exploring the interplay between renewable energy production and energy consumption in Northern Europe, 
while also examining the influence of weather patterns. Using open-source APIs from ENTSO-E and VisualCrossing, this project leverages cutting-edge tools to deliver valuable data-driven perspectives.

The energy markets and weather conditions are monitored through an automated data pipeline built using a Dockerized environment and orchestrated with Apache Airflow. 
Raw data is ingested into an Apache Iceberg data lakehouse, queried, and processed using dbt (data build tool) to meet specific analytical requirements. 
Further transformations, including the creation of star schemas within DuckDB, are managed by Airflow DAGs. 
These transformations prepare the necessary tables for an interactive Streamlit dashboard that facilitates data exploration and visualization.

## Features
- Automated data pipeline for seamless ingestion, transformation, and analysis.
- Interactive dashboard powered by Streamlit for real-time visualizations.
- In-depth insights into the relationship between renewable energy, weather, and market dynamics.
- Intuitive star-schema populated for ease of use for downstream users such as ML engineers or analysts

## Installation
1. Install Docker
2. Clone repository: 
3. Populate API keys into .env file based on env-sample found in PROTO directory
4. In /PROTO/config_files you can choose the time period of data ingestion in config_dates.json (for testing purposes request weekly duration at max)
5. In /PROTO/config_files country_code_mapper.csv you can choose which countries and/or electricity bidding zones to analyze
6. In /PROTO directory run 'docker compose up'
7. Open localhost:8080 to reach Airflow UI on your browser
8. RUN DAG 1_ to populate iceberg and duckdb with raw
9. RUN DAG 2_ to create star schema and enable Streamlit dashboard
10. Open Localhost:8501 to view Streamlit

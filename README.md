# End-to-End Formula 1 Data Engineering Project

This project is a comprehensive data pipeline that extracts historical Formula 1 data, processes it using modern data tools, and presents it in an interactive analytics dashboard.

The primary goal is to demonstrate a full ELT (Extract-Load-Transform) workflow, showcasing skills in data modeling, transformation, and visualization to turn raw API data into business-ready insights.

## 🏁 Final Dashboard Preview

The final output is an interactive Power BI dashboard connected directly to the Snowflake data warehouse. It provides insights into driver and team standings, performance metrics, pit stop analysis, and historical trends.

- **Key Features:**
  - Driver and Constructor championship standings for any season.
  - Head-to-head driver performance comparison (Qualifying vs. Race Day).
  - In-depth pit stop analysis, including average times and fastest stops per team.
  - Deep dive into race results with circuit details and fastest laps.

_(Action: Insert a screenshot or GIF of your final Power BI dashboard here!)_
``

## 🛠️ Tech Stack

This project utilizes a modern, cloud-focused data stack.

| Technology                | Area           | Description                                                                                        |
| ------------------------- | -------------- | -------------------------------------------------------------------------------------------------- |
| **Python**                | Data Ingestion | `requests` and `pandas` are used to extract data from the API and prepare it for loading.          |
| **Snowflake**             | Data Warehouse | A cloud-native data warehouse to store raw, staged, and final production-ready data marts.         |
| **dbt (Data Build Tool)** | Transformation | Manages the "T" in ELT. Transforms raw data into clean, tested, and analysis-ready models via SQL. |
| **Power BI**              | Visualization  | Connects to the final dbt models in Snowflake to create an interactive analytics dashboard.        |

## ⚙️ Data Flow

The project follows a classic ELT (Extract-Load-Transform) process:

1.  **Extract & Load:** A Python script in the `ingestion/` directory fetches data from the Ergast F1 API. It then loads this raw data directly into a `RAW_DATA` schema in Snowflake without any major changes.

2.  **Transform:** The dbt project (`f1_project/`) connects to Snowflake. It reads the raw data and executes a series of SQL models to clean, join, test, and aggregate the data. The output is a clean star schema (dimensions and facts) built in a final `PROD_DATA` schema.

3.  **Visualize:** Power BI connects directly to the final tables in the `PROD_DATA` schema. This ensures the dashboard is powered by clean, reliable, and analysis-ready data.

## 🔮 Future Improvements

While the current pipeline is fully functional, future enhancements could include:

- **Orchestration:** Integrating an orchestrator like **Apache Airflow** to automate the pipeline runs on a set schedule.
- **CI/CD:** Implementing a CI/CD pipeline (e.g., using GitHub Actions) to automatically test dbt models.
- **Deeper Analysis:** Ingesting more granular data, such as driver telemetry, using the [FastF1](https://theoehrly.github.io/Fast-F1/) library.

## 🙏 Acknowledgements

- This project would not be possible without the incredible and free **[Ergast Developer API](http://ergast.com/mrd/)**, which provides the historical F1 data.

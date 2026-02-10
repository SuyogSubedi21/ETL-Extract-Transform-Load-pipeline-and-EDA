📊 End-to-End Retail ELT & Exploratory Data Analysis

This project demonstrates an end-to-end data workflow, combining data engineering fundamentals with exploratory data analysis to extract business insights from retail sales data.

The work begins with a simple ELT pipeline, where data is extracted from a CSV source, transformed and validated, and loaded into both a cleaned CSV output and a PostgreSQL database. Logging is implemented throughout the pipeline to track execution flow, data loading status, and potential errors.

On top of the curated dataset, a structured EDA is performed to analyse:

Revenue and volume drivers at product and category level

Profitability patterns and loss-making segments

The impact of discounting on profit margins

Time-based trends across months and years

Geographic performance using a state-level profit/loss map

Visualisations are created using Python (pandas, matplotlib), along with an interactive Plotly choropleth map to highlight profit and loss distribution across U.S. states.

The project emphasises:

Correct metric definitions (e.g. ratios vs percentages)

Data quality checks before analysis

Thoughtful aggregation and visualisation choices

Clear, business-oriented interpretation of results

This repository serves as a practical example of connecting data ingestion, transformation, observability, and analytics in a single workflow.

🛠️ Tools & Technologies

Python (pandas, matplotlib)

PostgreSQL

Plotly (for interactive mapping)

Git & GitHub

Logging for pipeline observability

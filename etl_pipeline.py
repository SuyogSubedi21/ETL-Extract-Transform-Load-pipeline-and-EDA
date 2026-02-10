import pandas as pd
import requests
from io import StringIO
from datetime import datetime
from sqlalchemy import create_engine


#Logging function to track progress and errors
def log_progress(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("code_log.txt", "a", encoding="utf-8") as f:
        f.write(f"{timestamp} [{level}] : {message}\n")


#Extract data from CSV URL
def extract(csv_url):
    try:
        log_progress("Starting data extraction")

        response = requests.get(csv_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))

        log_progress(f"Data extraction completed. Rows: {len(df)}")
        return df

    except Exception as e:
        log_progress(f"Data extraction failed: {e}", level="ERROR")
        raise


#Transformation and cleaning
def transform(df):
    try:
        log_progress("Starting data transformation")

        # standardize column names
        df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_").replace(".", "_") for c in df.columns]

        # detect order date column
        if "order_date" in df.columns:
            date_col = "order_date"
        elif "orderdate" in df.columns:
            date_col = "orderdate"
        else:
            raise ValueError("No order date column found after cleaning column names.")

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

       
        for col in ["sales", "profit", "quantity", "discount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # drop rows with missing sales if sales exists
        if "sales" in df.columns:
            df = df.dropna(subset=["sales"])

        # derived metrics if possible
        if "sales" in df.columns and "profit" in df.columns:
            df["profit_margin"] = (df["profit"] / df["sales"]).round(2)

        df["order_year"] = df[date_col].dt.year
        df["order_month"] = df[date_col].dt.month

        log_progress(f"Data transformation completed. Rows: {len(df)}")
        return df

    except Exception as e:
        log_progress(f"Data transformation failed: {e}", level="ERROR")
        raise


#Load to CSV file
def load_to_csv(df, output_path):
    try:
        log_progress("Starting CSV load")

        df.to_csv(output_path, index=False)

        log_progress("CSV saved successfully")

    except Exception as e:
        log_progress(f"CSV load failed: {e}", level="ERROR")
        raise


#Load to PostgreSQL database
def load_to_db(df, table_name, engine):
    try:
        log_progress(f"Starting database load into table '{table_name}'")

        df.to_sql(table_name, engine, if_exists="replace", index=False)

        log_progress("Database load completed successfully")

    except Exception as e:
        log_progress(f"Database load failed: {e}", level="ERROR")
        raise

if __name__ == "__main__":

    SALES_CSV_URL = "https://raw.githubusercontent.com/leonism/sample-superstore/master/data/superstore.csv"
    OUTPUT_CSV = "./clean_sales_data.csv"
    TABLE_NAME = "sales_data"

    DB_USER = "postgres"
    DB_PASS = "subedi100"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "database"

    log_progress("Preliminaries complete. Initiating ETL process")

    engine = None
    try:
        engine = create_engine(
            "postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(
                DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME
            )
        )
        log_progress("PostgreSQL connection created")

        df = extract(SALES_CSV_URL)
        df = transform(df)
        load_to_csv(df, OUTPUT_CSV)
        load_to_db(df, TABLE_NAME, engine)

        log_progress("ETL process completed successfully")

    except Exception as e:
        log_progress(f"ETL process failed: {e}", level="ERROR")

    finally:
        log_progress("ETL process ended")

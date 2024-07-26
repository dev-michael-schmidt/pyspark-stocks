import sys
import requests
import csv
from io import StringIO
from datetime import datetime
from pyspark.sql.types import StructType, StructField, DateType, DoubleType
from pyspark.sql import Row
from pyspark.sql.functions import lit

from pyspark.sql import SparkSession

from technical_builder import TechnicalBuilder

if __name__ == "__main__":

    def string_to_date(date_str):
        return datetime.strptime(date_str, '%Y-%m-%d').date()

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("SparkPythonStocks") \
        .config("spark.master", "local") \
        .getOrCreate()

    # Parameters for Yahoo Finance API
    symbol = "SPY"
    period1 = 1709272800  # 2024-03-01 00:00:00
    period2 = 1717217999  # 2024-05-31 23:59:59
    interval = "1d"
    events = "history"

    # Request data from Yahoo Finance API
    url = (f"https://query1.finance.yahoo.com/v7/finance/download/"
           f"{symbol}?"
           f"period1={period1}&"
           f"period2={period2}&"
           f"interval={interval}&"
           f"events={events}")

    response = requests.get(url, headers={'User-agent': 'your bot 0.1'})
    response.raise_for_status()

    # Parse CSV data
    data = response.text
    csv_data = csv.reader(StringIO(data))
    rows = list(csv_data)

    # Extract header and data
    header = [col.lower().replace(' ', '_') for col in rows[0]]
    data_rows = rows[1:]

    # Create DataFrame rows
    row_data = []
    for row in data_rows:
        date = string_to_date(row[0])
        values = [float(val) for val in row[1:]]
        row_data.append(Row(date, *values))

    # Define schema
    schema = StructType(
        [StructField("date", DateType(), nullable=False)] +
        [StructField(name, DoubleType(), nullable=True) for name in header[1:]]
    )

    # Create DataFrame
    stock_history_df = spark.createDataFrame(row_data, schema) \
        .withColumn("symbol", lit(symbol))

    stock_history_df = (TechnicalBuilder(stock_history_df)
                        .SMA(10)
                        .EMA(10)
                        .build())

    # Show DataFrame
    stock_history_df.show()

    spark.stop()
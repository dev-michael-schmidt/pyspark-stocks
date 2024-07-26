from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import avg, col, pandas_udf, PandasUDFType
import pandas as pd


class TechnicalBuilder:
    def __init__(self, df: DataFrame):
        self.df = df

    def SMA(self, days: int = 10):
        window_spec = Window.orderBy("date").rowsBetween(-days + 1, 0)
        column_name = f"sma_{days}d"
        self.df = self.df.withColumn(column_name, avg(col("close")).over(window_spec))
        return self

    def EMA(self, span: int = 10):
        @pandas_udf("double", PandasUDFType.SCALAR)
        def ema_udf(close: pd.Series) -> pd.Series:
            return close.ewm(span=span, adjust=False).mean()

        column_name = f"ema_{span}d"
        self.df = self.df.withColumn(column_name, ema_udf(col("close")))
        return self

    def build(self):
        return self.df

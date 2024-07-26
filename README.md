This PySpark project fetches data from Yahoo Finance's public stock data API, parses the CSV and loads it into a PySpark DataFrame, and then performs (currently)
- Simple Moving Average (specify window in days)
- Exponential Moving Average (specify in days)

This DataFrame includes:
- Date  
- Opening price
- High price of the day
- Low price of the day
- The Closing price
- Symbol
- sma_10d  -- a 10-day simple moving average (default to 10, you can specify the span)
- ema_10d  -- a 10-day exponential moving average (defaults to 10, you can specify span)  
- More technical markers to come  

Example output
```  
+----------+---------+-------+------+-------+----------+------+--------------+---------------+
|      date|     open|   high|   low|  close|    volume|symbol|       sma_10d|        ema_10d|
+----------+---------+-------+------+-------+----------+------+--------------+---------------+
| 2024-03-01|  508.98| 513.28| 508.5| 512.84| 7.68059E7|   SPY|        512.84|        512.846|
| 2024-03-04|  512.03| 514.20| 512.0| 512.29| 4.97993E7|   SPY|        512.57|  512.749978118|
| 2024-03-05|  510.23| 510.70| 504.9| 507.17| 7.28556E7|   SPY| 510.776652333| 511.7372536057|
| 2024-03-06|  510.54| 512.07| 508.4| 509.75| 6.83824E7|   SPY|      510.5199|  511.375936341|
```

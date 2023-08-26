from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("DataVisualization").getOrCreate()

# Load CSV data into a DataFrame
data = spark.parquet.csv("s3://my-analytics-bucket-bdpro/dataset.parquet/part-00000-3930788b-acd0-4f05-8b76-8e588e6f62ec-c000.snappy.parquet", header=True, inferSchema=True)

# Time Series Analysis: Plot sales trends over time
data.createOrReplaceTempView("sales_data")
time_series_df = spark.sql("SELECT OrderDate, SUM(`Order Quantity`) AS TotalQuantity FROM sales_data GROUP BY OrderDate ORDER BY OrderDate")
time_series_pd = time_series_df.toPandas()
time_series_pd['OrderDate'] = pd.to_datetime(time_series_pd['OrderDate'])
plt.plot(time_series_pd['OrderDate'], time_series_pd['TotalQuantity'])
plt.xlabel("Order Date")
plt.ylabel("Total Quantity")
plt.title("Time Series Analysis: Sales Trends Over Time")
plt.xticks(rotation=45)
plt.show()

# Sales Channel Comparison: Compare sales across different channels
sales_channel_df = spark.sql("SELECT `Sales Channel`, SUM(`Order Quantity`) AS TotalQuantity FROM sales_data GROUP BY `Sales Channel`")
sales_channel_pd = sales_channel_df.toPandas()
sales_channel_pd.plot(kind='bar', x='Sales Channel', y='TotalQuantity', title='Sales Channel Comparison')

# Product Analysis: Visualize sales distribution across products
product_df = spark.sql("SELECT `ProductID`, SUM(`Order Quantity`) AS TotalQuantity FROM sales_data GROUP BY `ProductID`")
product_pd = product_df.toPandas()
product_pd.plot(kind='pie', y='TotalQuantity', labels=product_pd['ProductID'], autopct='%1.1f%%', legend=False)

# Discount Analysis: Visualize impact of discounts on sales
discount_df = spark.sql("SELECT `Discount Applied`, SUM(`Order Quantity`) AS TotalQuantity FROM sales_data GROUP BY `Discount Applied`")
discount_pd = discount_df.toPandas()
plt.scatter(discount_pd['Discount Applied'], discount_pd['TotalQuantity'])
plt.xlabel("Discount Applied")
plt.ylabel("Total Quantity")
plt.title("Discount Analysis: Impact on Sales")
plt.show()

# Regional Performance: Create maps to visualize sales performance across regions (requires geospatial data)
# You need geospatial data and appropriate libraries to create regional maps.
# Example: Use GeoPandas to create maps based on region information.

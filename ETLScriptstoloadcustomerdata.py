from pyspark.sql.functions import col
import pandas as pd

pdf = pd.read_excel("/dbfs/FileStore/tables/Sample_Superstore.xls")
df = spark.createDataFrame(df)


Customer = df.select(col("Customer ID"), col("Customer Name"), col("City"), col("State"), col("Country"), col("Postal Code"))

Product = df.select(col("Product ID"), col("Product Name"), col("Category"), col("Sub-Category"))

Sales_fact = df.select(col("Quantity"), col("Discount"), col("Profit"), col("Sales"))
Sales_fact = Sales_fact.withColumn("Unit_price", Sales_fact.Sales/Sales_fact.Quantity)

Segment = df.select(col("Segment"))

Segment = Segment.withColumnRenamed("Segment","Segment_type")

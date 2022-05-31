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
customerpdf = Customer.toPandas()
segmentpdf =  Segment.toPandas()
productpdf = Product.toPandas()
salesfactpdf = Sales_fact.toPandas()

l=[]
for i in range(1,Customer.count()+1):
  l.append(i)

customerpdf["Customer_SK"] = l
segmentpdf["Segment_SK"] = l
productpdf["Product_Sk"] = l
salesfactpdf["Customer_SK"] = customerpdf["Customer_SK"]
salesfactpdf["Segment_SK"] = segmentpdf["Segment_SK"]
salesfactpdf["Product_SK"] = productpdf["Product_Sk"]
salesfactpdf["Sales_Fact_SK"] = l

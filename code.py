#!/usr/bin/env python
# coding: utf-8

# ## Sales analytics
# 
# null

# In[1]:


from pyspark.sql.functions import *
from pyspark.sql.types import *


# In[2]:


bronze_path="abfss://31f1e6f6-275b-46d7-9090-8292f8d4f4c0@onelake.dfs.fabric.microsoft.com/17f290ca-345a-4dd7-b8f7-8f157894bda5/Files/bronze/retail_raw_data_560.parquet"
df_bronze=spark.read.parquet(bronze_path)
display(df_bronze)


# In[3]:


df_silver = (
    df_bronze
    .withColumnRenamed("Order iD", "OrderID")
    .withColumnRenamed("order_date", "OrderDate")
    .withColumnRenamed("cust_ID", "CustomerID")
    .withColumnRenamed("Cust Segment", "CustomerSegment")
    .withColumnRenamed("prodct_ID", "ProductID")
    .withColumnRenamed("Prodct Category", "ProductCategory")
    .withColumnRenamed("prod_Name", "ProductName")
    .withColumnRenamed("QTY", "Quantity")
    .withColumnRenamed("price_unit", "OrderAmount") 
    .withColumnRenamed("cost_unit", "CostUnit")
    .withColumnRenamed("new customer", "IsNewCustomer")
    .withColumnRenamed("store type", "StoreType")

    #Làm sạch dữ liệu cột Quantity 
    .withColumn(
        "Quantity",
        when(lower(col("Quantity")) == "one", 1)
        .when(lower(col("Quantity")) == "two", 2)
        .when(lower(col("Quantity")) == "three", 3)
        .otherwise(col("Quantity").cast(IntegerType()))
    )

    #Chuẩn hóa OrderDate theo nhiều định dạng
    .withColumn(
        "OrderDate",
        coalesce(
            to_date(col("OrderDate"), "yyyy/MM/dd"),
            to_date(col("OrderDate"), "dd-MM-yyyy"),
            to_date(col("OrderDate"), "MM-dd-yyyy"),
            to_date(col("OrderDate"), "yyyy.MM.dd"),
            to_date(col("OrderDate"), "dd/MM/yyyy"),
            to_date(col("OrderDate"), "dd.MM.yyyy"),
            to_date(col("OrderDate"), "MMMM dd yyyy")
        )
    )

    #Làm sạch ký tự tiền tệ  
    .withColumn("OrderAmount", regexp_replace(col("OrderAmount").cast("string"), "[$₹Rs.|USD|INR,]", ""))
    .withColumn("OrderAmount", col("OrderAmount").cast(DoubleType()))

    #Chuẩn hóa các cột text 
    .withColumn("region", lower(regexp_replace(col("region"), "[^a-zA-Z ]", "")))
    .withColumn("channel", lower(regexp_replace(col("channel"), "[^a-zA-Z ]", "")))
    .withColumn("StoreType", lower(regexp_replace(col("StoreType"), "[^a-zA-Z ]", "")))
    .withColumn("CustomerSegment", lower(regexp_replace(col("CustomerSegment"), "[^a-zA-Z ]", "")))

    #Điền giá trị mặc định nếu null
    .fillna({
        "Quantity": 0,
        "OrderAmount": 0.0,
        "region": "unknown",
        "channel": "unknown"
    })

    #Xóa hàng thiếu dữ liệu chính 
    .na.drop(subset=["CustomerID", "ProductName"])

    #Loại bỏ trùng lặp theo OrderID
    .dropDuplicates(["OrderID"])
)
display(df_silver)


# In[4]:


df_silver.write.mode("overwrite").saveAsTable("silver_retail_cleaned")


# In[7]:


df_silver = df_silver.withColumn("Year", year("OrderDate")) \
                     .withColumn("Quarter", quarter("OrderDate")) \
                     .withColumn("Month", month("OrderDate"))
df_gold = (
    df_silver.groupBy(
        "CustomerSegment", "ProductCategory", "Region", "StoreType",
        "Channel", "Year", "Quarter", "Month"
    ).agg(
        sum("OrderAmount").alias("TotalSales"),          
        avg("OrderAmount").alias("AvgUnitPrice"),        
        avg("CostUnit").alias("AvgCostPrice"),            
        sum("Quantity").alias("TotalQuantity"),
        countDistinct("CustomerID").alias("UniqueCustomers"),
        countDistinct("OrderID").alias("TotalOrders")
    ))

display(df_gold)


# In[8]:


df_gold.write.mode("overwrite").saveAsTable("gold_retail_segment_metrics")


# In[ ]:





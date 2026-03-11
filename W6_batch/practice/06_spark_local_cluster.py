#!/usr/bin/env python
# coding: utf-8

# In[1]:


import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


# In[2]:


spark = SparkSession.builder \
    .master("spark://Khanhs-MacBook-Pro.local:7077") \
    .appName('test') \
    .getOrCreate()


# In[3]:


spark


# In[4]:


df_green = spark.read.option("recursiveFileLookup", "true").parquet(
    input_green
)


# In[5]:


df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


# In[6]:


df_yellow = spark.read.option("recursiveFileLookup", "true").parquet(
    input_yellow
)


# In[7]:


df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


# In[14]:


common_columns = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)


# In[15]:


common_columns


# In[16]:


df_green.select(common_columns).show()


# In[17]:


from pyspark.sql import functions as F


# In[19]:


df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))


# In[20]:


df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))


# In[ ]:


df_trips_data = df_green_sel.unionAll(df_yellow_sel)


# In[ ]:


df_trips_data.groupBy('service_type').count().show()


# In[ ]:


df_trips_data.registerTempTable('trips_data')


# In[13]:


spark.sql("""
SELECT
    service_type,
    count(1)
FROM
    trips_data
GROUP BY 
    service_type
""").show()


# In[110]:


df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


# In[111]:


df_result.coalesce(1).write.parquet(output, mode='overwrite')



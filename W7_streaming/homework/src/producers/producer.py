#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"


# In[3]:


columns = ['PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'total_amount', 'tip_amount', 'lpep_pickup_datetime', 'lpep_dropoff_datetime']

df = pd.read_parquet(url, columns=columns)


# In[4]:


df['passenger_count'] = df['passenger_count'].fillna(0)


# In[5]:


print(f"Loaded {len(df)} rows")


# In[6]:


from models import Ride, ride_from_row, ride_serializer


# In[7]:


from kafka import KafkaProducer

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)


# In[8]:


topic_name = 'green-trips'


# In[9]:


import time

t0 = time.time()
sent = 0

for _, row in df.iterrows():
    ride = ride_from_row(row)


    ride.lpep_pickup_datetime = str(ride.lpep_pickup_datetime)
    ride.lpep_dropoff_datetime = str(ride.lpep_dropoff_datetime)

    sent += 1
    producer.send(topic_name, value=ride)
    time.sleep(0.01)

producer.flush()

t1 = time.time()

print(f"Total rows sent: {sent}") 
print(f'took {(t1 - t0):.2f} seconds')


# In[ ]:





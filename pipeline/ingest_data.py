#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[10]:


# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz')


# In[11]:


# Display first rows
df.head()


# In[7]:


# Check data types
df.dtypes


# In[8]:


# Check data shape
df.shape


# In[13]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates
)


# In[14]:


df.head()


# In[16]:


get_ipython().system('uv add sqlalchemy psycopg2-binary')


# In[17]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[18]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[19]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[31]:


df_iter = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    iterator=True,
    chunksize=100000
)


# In[28]:


from tqdm.auto import tqdm


# In[26]:


get_ipython().system('uv add tqdm')


# In[32]:


for df_chunk in tqdm(df_iter):
    print(len(df_chunk))
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


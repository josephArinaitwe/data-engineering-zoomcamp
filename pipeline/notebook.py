#!/usr/bin/env python
# coding: utf-8

# In[13]:


import pandas as pd


# In[14]:


prefix= 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
url=f'{prefix}/yellow_tripdata_2021-01.csv.gz'


# In[15]:


df = pd.read_csv(url)


# In[16]:


len(df)


# In[23]:


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
    url,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[21]:


df.head()


# In[24]:


df


# In[25]:


df['tpep_pickup_datetime']


# In[26]:


get_ipython().system('uv add sqlalchemy')


# In[29]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[30]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[28]:


get_ipython().system('uv add psycopg2-binary')


# In[31]:


df.head


# In[32]:


df.head(0)


# In[33]:


df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[36]:


df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000,)


# In[57]:


for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append', index = False)


# In[40]:


df = next(df_iter)


# In[46]:


from tqdm.auto import tqdm


# In[62]:


df = next(df_iter)


# In[61]:


df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000,)

for df_chunk in tqdm(df_iter, desc="Ingesting data"):
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append",
    )
    print("Inserted chunk:", len(df_chunk))


# In[ ]:





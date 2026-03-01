#!/usr/bin/env python
# coding: utf-8

# ### Workshop ➜ Homework task.

# In[12]:

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import PageNumberPaginatorConfig

# ----

# #### Define the API.

# In[2]:


def ny_taxi_source():
    return rest_api_source({
        "client": {
            "base_url":
            "https://us-central1-dlthub-analytics.cloudfunctions.net",
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "rides",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        "total_path": None,
                    },
                },
            },
        ],
    })


# ---

# #### Define the pipeline.

# In[3]:

pipeline = dlt.pipeline(
    pipeline_name="ny_taxi",
    destination="duckdb",
    dataset_name="ny_taxi_data",
    progress="log",
)

# ---

# #### Extract.

# In[4]:

extract_info = pipeline.extract(ny_taxi_source())

# ---

# In[5]:

load_id = extract_info.loads_ids[-1]
m = extract_info.metrics[load_id][0]

print("Resources:", list(m["resource_metrics"].keys()))
print("Tables:", list(m["table_metrics"].keys()))
print("Load ID:", load_id)
print()

for resource, rm in m["resource_metrics"].items():
    print(f"Resource: {resource}")
    print(f"rows extracted: {rm.items_count}")
    print()

# ---

# #### Normalization.

# In[6]:

normalize_info = pipeline.normalize()
load_id = normalize_info.loads_ids[-1]
m = normalize_info.metrics[load_id][0]

print("Load ID:", load_id)
print()

print("Tables created/updated:")
for table_name, tm in m["table_metrics"].items():
    # skip dlt internal tables to keep it beginner-friendly
    if table_name.startswith("_dlt"):
        continue
    print(f"  - {table_name}: {tm.items_count} rows")

# ---

# #### Load.

# In[7]:

load_info = pipeline.load()

# ---

if __name__ == "__main__":
    ds = pipeline.dataset()
    print(ds.tables)

    df = ds.rides.df()
    print(df.head(10))

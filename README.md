# Data-Engineering-Projects

![DataEngineering](https://user-images.githubusercontent.com/19205616/119996332-45d97b80-bfec-11eb-9b8d-0d436de45b0a.jpg)

*Credit @RealPython*

**Data Engineering is a field of Data Science having ultimate goal to provide Organized & Consistent Data flow to enable data-driven decisions.**

This Data flow can be achieved in a number of ways, using specific tool sets, techniques and skills which will vary across teams & organizations and the Data architecture within the company. But the most common pattern is creating an **ETL Pipeline**.

## ETL Pipeline (Extract - Transform - Load)

Data Pipelines can be distributed over multiple servers. Below is one common ETL Pipeline, but it can be varied depending on different Sources, and their usages.

![ETL](https://user-images.githubusercontent.com/19205616/119997149-0a8b7c80-bfed-11eb-98d9-9182621c602c.jpg)

The Data can come from any Source: it can either be from Stored Files or Streaming Data from IoT Devices, System Logs, User Actvity on an App, etc.

### E (Extraction)
Extraction takes care of extracting the data from different Sources, stored or streaming data.

### T (Transformation)
Transformation takes care of processing the data, identifying and removing anomalies, decides schema according to the Data.

### L (Loading)
Loading takes care of storing the data to the Storage either Data Mart, Data Warehouse, etc. which will later be utilized by multiple teams as per their requirement.

Further References to study about Data Engineering and Skills required:
1. https://realpython.com/python-data-engineer/
2. https://github.com/datastacktv/data-engineer-roadmap

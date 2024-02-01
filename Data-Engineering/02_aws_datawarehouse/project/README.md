<p align="center">
  <a href="" rel="noopener">
 <img width=200px height=200px src="https://i.imgur.com/6wj0hh6.jpg" alt="Project logo"></a>
</p>

<h3 align="center">Project- AWS Data Warehouse</h3>

<div align="center">

[![Status](https://img.shields.io/badge/status-active-success.svg)]()
[![GitHub Issues](https://img.shields.io/github/issues/kylelobo/The-Documentation-Compendium.svg)](https://github.com/kylelobo/The-Documentation-Compendium/issues)
[![GitHub Pull Requests](https://img.shields.io/github/issues-pr/kylelobo/The-Documentation-Compendium.svg)](https://github.com/kylelobo/The-Documentation-Compendium/pulls)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

</div>

---

<p align="left"> 
    A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

  As a data engineer, I am building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.<br> 
</p>

## 📝 Table of Contents

- [About](#about)
- [Getting Started](#getting_started)
- [Usage](#usage)

## 🧐 About <a name = "about"></a>

Extract a few json format data from aws s3 into data warehouse. The purpose database is to facilitate data warehouse can empower data scientists to get insights from data. 
Under the data warehouse, we use star schemas to design data warehouse. There will be three types of schemas to apply the star schemas. 
* staging
`staging` schema is to store raw data at json format into data base, which are `staging_events` and `staging_songs`.
* dim
Based on the `staging_events` and `staging_songs`, we need to split them into four dimension tables under `dim` schema, which are `dim_users`, `dim_songs`, `dim_artists` and `dim_time` individually. 
* fact
With the dim tables, a fact table `fact_song_plays` is created under `fact` schema. 

ETL process is to extract raw data to fact tables. Below are different python file to enable the ETL
* create_schemas.py
The file is to drop schemas if they are not exist and create them. 

* create_tables.py
The file is to drop tables if they are not exist and create them. 

* etl.py
The file is to copy the raw data from s3 into staging tables, insert staging data into individual dim tables and finally join dim tables to create fact tables. 

## 🏁 Getting Started <a name = "getting_started"></a>

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See [deployment](#deployment) for notes on how to deploy the project on a live system.

### Prerequisites

* Docker Container <br>
Docker Container is used to run the environment for the project. 

* AWS Account <br>
As the data is stored in AWS and Data Warehouse is deployed in AWS, you need to have AWS account before running this project

### Installing

A step by step series of examples that tell you how to get a development env running.

* Download Docker. [Download Here](https://www.docker.com/products/docker-desktop/)

* Run the docker container under the folder `data-engineering/02_aws_datawarehouse/project` after its installation

```
cd data-engineering/02_aws_datawarehouse/project
docker compose up -d
```

* Stop the docker container 
```
docker compose down
```
* Down VScode

* Install Extension `Dev Containers` [extension id is `ms-vscode-remote.remote-containers`]

* Open `Dev Containers` and then open folder `data-engineering`. Be aware the default folder would be `/root/`. Please remove it and then you will see `data-engineering` and click it. 

## 🎈 Usage <a name="usage"></a>

* Create a `dwh.cfg` file under the directory `02_aws_datawarehouse/project` and fill the `KEY` and `SECRET`. Below is the sample. 
```
[CLUSTER]
DB_NAME=dwh
DB_USER=dwhuser
DB_PASSWORD=Passw0rd
DB_PORT=5439
DWH_IAM_ROLE_NAME=dwhRole
DWH_CLUSTER_IDENTIFIER=dwhCluster
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
KEY=#to be filled
SECRET=#to be filled
```
* Run the project in cmd. Be aware that the running time could be 5 ~ 10 minutes.
```
cd data-warehouse-project # make sure you run the script under the directory
python main.py
```
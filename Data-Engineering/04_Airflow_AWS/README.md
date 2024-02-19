# Project Title

## Table of Contents

- [About](#about)
- [Getting Started](#getting_started)
- [Usage](#usage)

## About <a name = "about"></a>

The project is to build up a data pipeline via airflow. 

## Getting Started <a name = "getting_started"></a>

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See [deployment](#deployment) for notes on how to deploy the project on a live system.

### Prerequisites

* Make sure Docker has been download in your local machine

### Installing

* Run the docker-compose.yml to start airflow. It will takes up to 5 minutes to finish the process. 

```
Docker compose up --build
```

## Usage <a name = "usage"></a>

* Go to your browser and fill the url `http://0.0.0.0:8080/`. 
* In the ariflow login page, the username is `airflow` and password is `airflow`.
* Make sure you need to add connections called `aws_credentials` and `redshift` in the `Admin -> Connections`.
* After it, go to the DAG named `final_project` and run it. 
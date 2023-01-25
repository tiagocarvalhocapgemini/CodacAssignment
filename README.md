# Pyspark Assignment - Tiago Carvalho

## Contents
* [Prerequisites](#pre)
* [Introduction](#intro)
* [Overview](#ov)
* [Data](#data)
* [Tasks](#tasks)

<a id="pre"></a>
## Prerequisites
* Visual Studio Code
* Pyspark >= 3.3
* Github account

<a id="intro"></a>
## Introduction
This project is part of the Pyspark Upskilling program with the objective of developing Pyspark and CI skills. The overall task is to grab 2 files locally and perform data filtering, to join dataframes and to upload the final data in a csv file into Github. The uploading into Github is done with an automated pipeline using GitHub Actions.

For more information see the tasks below.

<a id="ov"></a>
## Overview
A very small company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

Since all the data in the datasets is fake and this is just an exercise, one can forego the issue of having the data stored along with the code in a code repository.

<a id="data"></a>
## Data
The data is provided by indicating the path to the csv files in the local machine but follows the structure below.

### Client Data
|name|description|type|
|--|--|--|
|id|unique identifier|int|
|first_name|client's first name|string|
|last_name|client's last name|string|
|email|client's email|string|
|country|client's home country|string|

### Financial Data
|name|description|type|
|--|--|--|
|id|unique identifier|int|
|btc_a|bitcoin address|string|
|cc_t|credit card type|string|
|cc_n|credit number|int|

<a id="tasks"></a>
## Tasks
* Use WSL or a Linux VM
* Do **NOT** use notebooks (eg. Jupyter or Databricks)
* Only use clients from the **United Kingdom** or the **Netherlands**.
* Remove personal identifiable information from the first dataset, **excluding emails**.
* Data should by joined using **id** column
* Rename columns using below mapping:

|Old name|New name|
|--|--|
|id|client_identifier|
|btc_a|bitcoin_address|
|cc_t|credit_card_type|

* Store project in GitHub with only the relevant files uploaded
* Save a result dataset in client_data directory in the root directory of project
* Add **README** file
* Application should recieve 3 arguments: 2 path to datasets and a list of countries to preserve
* Use **logging**
* Test the application (Use https://github.com/MrPowers/chispa for Spark tests)
* Create generic functions for data filtering and columns renaming
* Build an automated pipeline using GitHub Actions
* Requirements file should exist.
* Document the code with docstrings as much as possible using the reStructuredText (reST) format.


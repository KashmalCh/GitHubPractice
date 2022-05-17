## Read ME DBT

## What the project does:
###### DBT allows us to develop and implement transformations to our data warehouse, Big Query, quickly using software engineering principals and integrations like GitHub. This DBT Repository is a centralized store of all DBT project code. Multiple projects live in this repository including, Simon Data and the Pricing Grid. For each of these projects DBT runs transformations/models the Big Query data and outputs a final output table to Big Query.

## Why the project is useful:
###### By integrating GitHub into our transformation processes we can practice proper version control. Furthermore, DBT allows for the use of macros and extends SQL using the jinja language. This can reduce the use of redundant code. 
#### Macro: 
	{% macro case_condition_week_maxweek_dayrank_maxday_kpi(fyearweek, MaxFyearWeek, DayRank, MaxDay, kpi) %}
	    CASE 
	        WHEN {{fyearweek}} = ({{MaxFyearWeek}}-100) AND {{DayRank}} < {{MaxDay}}
	            THEN {{kpi}}
	        WHEN {{fyearweek}} <> ({{MaxFyearWeek}}-100) AND {{DayRank}} <= 7 
	            THEN {{kpi}}
	        ELSE 
	            NULL 
	    END
	{% endmacro %}

#### Macro Call in Query: 
	SUM({{case_condition_week_maxweek_dayrank_maxday_kpi('fsw.fyearweek', 'dr.MaxFyearWeek', 'dr.DayRank', 'dr.MaxDay', 'fsw.sales')}}) WrSales,

## How users can get started with the project:
###### To get started you can reach out the Data Engineering team at mattress firm to get access. Each projects content is stored in common folders, i.e. all the model definitions for Simon-Data project are stored under models/Simon-Data. It is important to both understand how these projects are structured and what projects we are working on.

### Repo Structure:
#### macros : 
###### Contains the resuable macros which would be used in the project.
#### model : 
###### This folder contains all the code related files which are deployed in dbt .
#### seeds: 
###### Seeds are CSV files in your dbt project (typically in your data directory) that dbt can load into your data warehouse using the dbt seed command.
#### snaphot :
###### In dbt, snapshots are select statements, configure your snapshot to tell dbt how to detect record changes.
#### test: 
###### This folder we would write test cases which could be generic or sigular test cases on a model.
#### dbt_project.yml- 
###### This is how dbt knows a directory is a dbt project. This file also contains important information that tells dbt how to operate on your project.
#### packages.yml:
###### This file we will load packages which we could use in our models . 

### Projects in DBT:
#### Simon-Data: 
###### Simon-data project is built for the table which has the mapping for customer,product and sales relation . It has another table which has the customer data and the attributes which describes the customer.
#### Live-Ramp:
###### This is monthly customer demo append table.
#### Pricing-Grid:
###### This has the code for the pricing grid project on DBT. This is the project where we converted our Stored procedure running on BQ into DBT.
#### Analytics-Mart :
###### This is the analytics mart project which has the queries related to the

## Where users can get help with your project
###### To get help with a specific DBT project or DBT in general please reach out to the Data Engineering team at mattress firm.

## Who maintains and contributes to the project
###### The Data Engineering team maintains and contributes to the projects.

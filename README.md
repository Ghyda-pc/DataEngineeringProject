# DE_Assignment2

**This project DONE by:** 
*Ghayd'a Al Hyasat, Shrouq Al-Fuqaha'a, Ibrahim Shahbaz, Mawada*

**Prepared for:**
*Dr.Ibrahim Abu Alhaol* 
##
##
#### This project creates a daily updated pipline for visualizing COVID-19 situation on a world map simulation,
#### investigates the monthly vaccination progress and impact on total COVID-19 cases curve saturation in Jordan,
#### and bulit an LSTM  model to forcast new cases of COVID-19 in Jordan 
##
## This Project was bulit on Docker containers using the following open-source tools:
* Apache Airflow ( two dags )  
* Postgres Data-Base 
* Kibana and ElasticSearch 

#### After cloning this repo , you can run this Project on your machine, execute the following commands on the CLI :
1. Set the working directory to "DE_Project" folder: 

2. Make the volume folders 
````
mkdir ./dags ./logs ./plugins ./data ./output
````

3. Initiate Air-Flow 

````
docker-compose up airflow-init

````

4- Run the whole pipline on Airflow: 

````
docker-compose up -d # to run application 

````

5- Now you can access the Airflow webserver on http://localhost:8080/ 
                      the Pgadmin is on http://localhost:8000/
    note : all passwords configuration are in the docker-compose yaml file 

6- All generated plots will be available in the "output" mounted folder  
   All generated CSVs will be available in the "data" mounted folder 

7- Set the working directory to "elastic-docker" folder: 

8- Run Kibana and ElasticSearch: 
    
    ````
    docker-compose up -d # to run application 

    ````
9 - Now you can access Kibana on http://localhost:5601/ 
    note: you can see the visulations by openning the Dashboard

10- To close the containers and images for both file:  
docker-compose down #to terminate application

########################## THANK YOU  ################################

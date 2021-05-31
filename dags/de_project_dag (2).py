from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime
import pandas as pd



def _cases_data():
    import pandas as pd 
    cases = pd.read_csv("https://covid.ourworldindata.org/data/owid-covid-data.csv",parse_dates=['date'])
    cases['date'] = pd.to_datetime(cases['date'])
    cases.to_csv('/opt/airflow/data/Covid_Global_Casses.csv')

def _vaccines_data():
    vaccination_global = pd.read_csv('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv',parse_dates=['date'])
    vaccination_global['date'] = pd.to_datetime(vaccination_global['date'])
    vaccination_global.to_csv('/opt/airflow/data/Covid_Global_Vaccination.csv')
    
def _pre_process():
    Global_Cases = pd.read_csv('/opt/airflow/data/Covid_Global_Casses.csv', parse_dates=['date'])
    Global_Cases['date'] = pd.to_datetime(Global_Cases['date'])
    Global_Vaccination = pd.read_csv('/opt/airflow/data/Covid_Global_Vaccination.csv', parse_dates=['date'])
    Global_Vaccination['date'] = pd.to_datetime(Global_Vaccination['date'])
   
    Global_Vaccination.fillna(0,inplace=True)
    Jordan_Cases=Global_Cases[(Global_Cases['location']=='Jordan')]
    Jordan_Cases.fillna(0,inplace=True)
    cols=['date','iso_code' ,'new_cases' ,'new_deaths' ,'new_tests','total_cases','total_deaths']
    Jordan_Cases=Jordan_Cases[cols]
    vaccination_Jordan=Global_Vaccination[(Global_Vaccination['location']=='Jordan')]
    Jordan_Cases.to_csv('/opt/airflow/data/Covid_Jordan_Cases.csv')
    vaccination_Jordan.to_csv('/opt/airflow/data/Covid_Jordan_Vaccination.csv')
    
def Join_Datasets():
    Jordan_Cases = pd.read_csv('/opt/airflow/data/Covid_Jordan_Cases.csv', parse_dates=['date'])
    Jordan_Cases['date'] = pd.to_datetime(Jordan_Cases['date'])
    Jordan_Vaccination = pd.read_csv('/opt/airflow/data/Covid_Jordan_Vaccination.csv', parse_dates=['date'])
    Jordan_Vaccination['date'] = pd.to_datetime(Jordan_Vaccination['date'])
    Jordan_Cases_and_Vaccination=pd.merge(Jordan_Cases, Jordan_Vaccination, on='date', how='inner')
    Jordan_Cases_and_Vaccination.to_csv('/opt/airflow/data/Covid_Jordan_Merged_Vacc_Cases.csv')


def Scaling():
  import datetime
  import pandas as pd
  Covid_data=pd.read_csv('/opt/airflow/data/Covid_Jordan_Merged_Vacc_Cases.csv', parse_dates=['date'])
  Covid_data['month'] = Covid_data['date'].dt.month_name()
  Covid_data['month_number'] = Covid_data['date'].dt.month
  from sklearn.preprocessing import StandardScaler
  ss= StandardScaler(with_mean=False)
  CoivdJo=Covid_data.copy()
  Select_Columns=['total_cases','people_fully_vaccinated','new_cases']
  CoivdJo=CoivdJo[Select_Columns]
  DF_Jordan_u_3 = pd.DataFrame(ss.fit_transform(CoivdJo),columns=Select_Columns)
  CoivdJo2=Covid_data.copy()
  CoivdJo2.drop(['total_cases','people_fully_vaccinated','iso_code_y','new_cases'],inplace=True,axis=1)
  DF_Jordan_new =pd.merge(CoivdJo2, DF_Jordan_u_3, left_index=True, right_index=True,how='inner')
  DF_Jordan_new.to_csv('/opt/airflow/data/Covid_Jordan_Merged_Scaled_Vacc_Cases.csv')

def plot1():
  import matplotlib.pyplot as plt
  DF_Jordan_new=pd.read_csv('/opt/airflow/data/Covid_Jordan_Merged_Scaled_Vacc_Cases.csv', parse_dates=['date'])
  df_daily=DF_Jordan_new.groupby('month_number').agg(['max']).reset_index()
  df_daily.plot(x='month_number', y=['people_fully_vaccinated', 'total_cases'],kind="bar")
  labels = ['Jan', 'Feb', 'Mar','Apr','May']
  x=df_daily['month_number']-1
  plt.xticks(x, labels, rotation='horizontal')
  plt.legend(['People Fully Vaccinated', 'Total Cases'])
  plt.xlabel('Month')
  plt.title('Scaled Covid19 (Vaccination Vs Cases) for Jordan \n')
  plt.rcParams['figure.figsize'] = [10, 8]
  plt.savefig('/opt/airflow/output/Jordan_Vaccination_Vs_Cases.png')


def plot2():

  import pycountry
  import plotly
  import plotly.express as px
  import pandas as pd

  URL_DATASET = r'https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv'
  df1 = pd.read_csv(URL_DATASET)
  list_countries = df1['Country'].unique().tolist()

  d_country_code = {}  
  for country in list_countries:
      try:
          country_data = pycountry.countries.search_fuzzy(country)
          country_code = country_data[0].alpha_3
          d_country_code.update({country: country_code})
      except:
          print('could not add ISO 3 code for ->', country)
          d_country_code.update({country: ' '})

  for k, v in d_country_code.items():
      df1.loc[(df1.Country == k), 'iso_alpha'] = v

  fig = px.choropleth(data_frame = df1,
                      locations= "iso_alpha",
                      color= "Confirmed",  # value in column 'Confirmed' determines color
                      hover_name= "Country",
                      color_continuous_scale= px.colors.diverging.RdYlGn[::-1],  #  color scale red, yellow green
                      animation_frame= "Date")

  plotly.offline.plot(fig, filename=r'/opt/airflow/output/World_Cases_Map.html')


def CSV_to_Postgres():
    import sqlalchemy
    from sqlalchemy import create_engine
    import pandas as pd

    Global_Cases = pd.read_csv('/opt/airflow/data/Covid_Global_Casses.csv', parse_dates=['date'])
    Global_Vaccination = pd.read_csv('/opt/airflow/data/Covid_Global_Vaccination.csv', parse_dates=['date'])
    Covid_data=pd.read_csv('/opt/airflow/data/Covid_Jordan_Merged_Vacc_Cases.csv', parse_dates=['date'])
    DF_Jordan_new=pd.read_csv('/opt/airflow/data/Covid_Jordan_Merged_Scaled_Vacc_Cases.csv', parse_dates=['date'])

    host="postgres"
    database="airflow"
    user="airflow"
    password="airflow"
    port='5432'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    Global_Cases.to_sql('Covid19_Global_Cases', engine,if_exists='replace',index=False)
    Global_Vaccination.to_sql('Covid19_Global_Vaccination', engine,if_exists='replace',index=False)
    Covid_data.to_sql('Covid19_Jordan_Vaccination_Cases', engine,if_exists='replace',index=False)
    DF_Jordan_new.to_sql('Covid19_Jordan_Vaccination_Cases_Scaled', engine,if_exists='replace',index=False)   


    

with DAG("Covid19_ETL_DE_Project", start_date=datetime(2021, 5, 26),
    schedule_interval="@daily", catchup=False) as dag:
    
        get_covid_cases_data=PythonOperator(
            task_id="Extract_Covid_Casses_Data",
            python_callable=_cases_data
        )
        
        get_vaccination_data=PythonOperator(
            task_id="Extract_Covid_Vaccination_Data",
            python_callable=_vaccines_data
        )
        
        pre_processing=PythonOperator(
            task_id="pre_processing",
            python_callable=_pre_process
        )

        merge_datasets=PythonOperator(
            task_id="Join_Datasets",
            python_callable=Join_Datasets
        )
        Scaling=PythonOperator(
            task_id="Scale_Merged_Datset",
            python_callable=Scaling
        )
        Plot1=PythonOperator(
            task_id="Plotting_Jordan_Cases_Vs_Vaccinations",
            python_callable=plot1
        )
        Plot2=PythonOperator(
            task_id="Plotting_World_Casses_Map",
            python_callable=plot2
        )


        toPostgres = PythonOperator(task_id='to_Postgres', python_callable=CSV_to_Postgres)

        Install_dependecies = BashOperator(task_id='installing',bash_command='pip install sklearn matplotlib')
        Install_dependecies1 = BashOperator(task_id='installing1',bash_command='pip install pycountry chart_studio')


Install_dependecies >>Install_dependecies1>> [get_covid_cases_data,get_vaccination_data]>>pre_processing >> merge_datasets >> Scaling >> [Plot1,Plot2,toPostgres]
from datetime import date
from statistics import correlation
import string
from turtle import st
from sqlalchemy import create_engine, text
import pandas as pd
from pathlib import Path
import numpy as np

# Must install package matplotlib
import matplotlib.pyplot as plt

def read_given_file(f,use_sem_col):
    # reads CSV file into a pandas dataframe
    #return pd.read_csv(f,  sep=';',comment='#',skiprows=[1])    
    if use_sem_col:
      return pd.read_csv(f,  sep=';',comment='#',dtype='unicode')
    else:
      return pd.read_csv(f,  sep=',',comment='#',dtype='unicode')

try:
    '''**********************************************************************
    Main unit for Data sets 
    '''


    #Exercise 1:
    # *********************************** 
    # . Define some constants           *
    # ***********************************
    TXT_ = '.txt'; CSV_ = '.csv'; QTS_ = '"'; SLASH_ = '\\'

    #------------------------------------------
    # below Weather data
    #------------------------------------------      
    WEATHER_ = 'weather_data_2020'
    my_path         = str(Path(__file__).parent)+'/'
    common_path     = str(Path(__file__).parent)+'/'
    SQLITE_SRV      = 'sqlite:///'; DB_NAME_ = 'weatherdata.db'
    print("reading " + common_path + WEATHER_ + CSV_)

    #----------------------------------------
    # C O N N E C T I N G  T O   S Q L I T E
    #---------------------------------------
    print(SQLITE_SRV + my_path + DB_NAME_)
    #exit() 
    engine       = create_engine(SQLITE_SRV + my_path + DB_NAME_, echo=False)
    sqlite_conn  = engine.connect()
    if not sqlite_conn:
        print("DB connection is not OK!")
        exit()
    else:
        print("DB connection is OK.")
    sqlite_conn.execute(
        'DROP TABLE IF EXISTS Place'
    )
    sqlite_conn.execute(
        'DROP TABLE IF EXISTS Observation'
    )
    sqlite_conn.execute(
        'DROP TABLE IF EXISTS Temperature'
    )
    sqlite_conn.execute(
        'CREATE TABLE Place('
        'code TEXT PRIMARY KEY,'
        'name TEXT NOT NULL,'
        'latitude FLOAT NOT NULL,'
        'longitude FLOAT NOT NULL'
        ')'
    )
    sqlite_conn.execute(
        'CREATE TABLE Observation('
        'place TEXT NOT NULL,'
        'date DATE NOT NULL,'
        'rain FLOAT,'
        'snow FLOAT,'
        'airtemp FLOAT,'
        'groundtemp FLOAT,'
        'PRIMARY KEY (place, date),'
        'FOREIGN KEY (place) REFERENCES Place(code)'
        ')'
    )
    sqlite_conn.execute(
        'CREATE TABLE Temperature('
        'place TEXT NOT NULL,'
        'date DATE NOT NULL,'
        'lowest FLOAT,'
        'highest FLOAT,'
        'PRIMARY KEY (place, date),'
        'FOREIGN KEY (place) REFERENCES Place(code)'
        ')'
    )


    #Exercise 2:

    #------------------------------------
    # R E A D I N G  C S V   F I L E S
    #------------------------------------
    df_             = read_given_file(common_path + WEATHER_ + CSV_,False)
    date_column     = df_["year"].astype(str)+'-'+df_["month"].astype(str)+'-'+df_["day"].astype(str)
    df_.insert(0,"date",date_column)
    print("Data loaded:")
    print(df_)
    print("\n")

    #------------------------------------
    # D A T A  S A N I T I Z A T I O N
    #------------------------------------
    df_                 = df_.drop(['year', 'month', 'day', 'time', 'timezone'], axis=1)
    df_.columns         = ['date', 'rain', 'snow', 'airtemp', 'groundtemp', 'highest', 'lowest', 'place', 'code', 'latitude', 'longitude']
    df_['date']         = pd.to_datetime(df_['date'])
    df_['date']         = df_['date'].dt.date
    df_['rain']         = df_['rain'].astype(float).replace(-1.0, 0)
    df_['snow']         = df_['snow'].astype(float).replace(-1.0, 0)
    df_['airtemp']      = df_['airtemp'].astype(float)
    df_['groundtemp']   = df_['groundtemp'].astype(float)
    df_['highest']      = df_['highest'].astype(float)
    df_['lowest']       = df_['lowest'].astype(float)
    df_['latitude']     = df_['latitude'].astype(float)
    df_['longitude']    = df_['longitude'].astype(float)

    #data for table Place
    df_place            = df_.filter(['code','place','latitude','longitude'], axis=1).drop_duplicates().reset_index(drop=True)
    df_place.columns    = ['code','name','latitude','longitude']

    #data for table Observation
    df_observation          = df_.filter(['code','date','rain','snow','airtemp','groundtemp'], axis=1)
    df_observation.columns  =['place','date','rain','snow','airtemp','groundtemp']
    df_observation          = df_observation.groupby(['place','date']).apply(lambda x : x.ffill().bfill())
    df_observation          = df_observation.drop_duplicates(subset=['place', 'date']).reset_index(drop=True)

    #data for table Temperature
    df_temp             = df_.filter(['code', 'date', 'lowest', 'highest'], axis=1).drop_duplicates(subset=['code', 'date']).reset_index(drop=True)
    df_temp.columns     =['place','date','lowest','highest']
    
    print(list(df_.columns))
    print("\n")
    print(list(df_.dtypes))
    print("\n")
    print("Analyzed, found " +  str(df_.shape[0]) + " rows; " + str(df_.shape[1]) + " cols.")
    print("\n")
    print("Data for table Place:")
    print(df_place)
    print("\n")
    print("Data for table Observation:")
    print(df_observation)
    print("\n")
    print("Data for table Temperature:")
    print(df_temp)
    print("\n")

    #load data into tables
    df_place.to_sql('Place', sqlite_conn, if_exists='append', index=False)
    df_observation.to_sql('Observation', sqlite_conn, if_exists='append', index=False)
    df_temp.to_sql('Temperature', sqlite_conn, if_exists='append', index=False)



    #Exercise 3:

    #Exercise 3 a)
    sqla1 = """
          SELECT COUNT(date) AS count, name FROM Observation JOIN Place ON(Observation.place = Place.code) WHERE snow>0 AND snow IS NOT NULL GROUP BY(place)
          """
    testa1 = pd.read_sql_query(sqla1, sqlite_conn)
    print("Number of snowy days for each location with Null values excepted:")
    print(testa1)
    print("\n")

    sqla2 = """
          SELECT MAX(count), name FROM (SELECT COUNT(date) AS count, name FROM Observation JOIN Place ON(Observation.place=Place.code) WHERE snow>0 AND snow IS NOT NULL GROUP BY(place))
          """
    testa2 = pd.read_sql_query(sqla2, sqlite_conn)
    print("Location has most snowy days:")
    print(testa2)
    print("\n")

    sqla3 = """
          SELECT month, MAX(sum), name FROM (SELECT strftime('%m', date) AS month, SUM(snow) AS sum, name FROM Observation JOIN Place ON(Observation.place = Place.code) WHERE place = (SELECT place FROM (SELECT MAX(count),place FROM (SELECT COUNT(date) AS count, place FROM Observation WHERE snow>0 AND snow IS NOT NULL GROUP BY(place)))) GROUP BY(strftime('%m', date)))
          """
    testa3 = pd.read_sql_query(sqla3, sqlite_conn)
    print("The month with most snow of most snowy location:")
    print(testa3)
    print("\n")

    sqla4 = """
          SELECT month, MAX(sum) FROM (SELECT strftime('%m', date) AS month, COUNT(date) AS sum FROM Observation WHERE snow>0 AND place = (SELECT place FROM (SELECT MIN(count),place FROM (SELECT COUNT(date) AS count, place FROM Observation WHERE snow>0 AND snow IS NOT NULL GROUP BY(place)))) GROUP BY(strftime('%m', date)))
          """
    testa4 = pd.read_sql_query(sqla4, sqlite_conn)
    print("The month with most snowy days of least snowy location:")
    print(testa4)
    print("\n")



    #Exercise 3 b)

    print("Correlation of highest and lowest temperature over all places:")
    correlationb = df_temp['lowest'].corr(df_temp['highest'])
    print(correlationb)
    print("\n")

    print("Correlation of highest and lowest temperature in Helsinki-Vantaa Airport:")
    firstb = df_temp[df_temp['place']=='100968']
    correlationb2 = firstb['lowest'].corr(firstb['highest'])
    print(correlationb2)
    print("\n")

    print("Correlation of highest and lowest temperature in Utsjoki:")
    secondb = df_temp[df_temp['place']=='102035']
    correlationb3 = secondb['lowest'].corr(secondb['highest'])
    print(correlationb3)
    print("\n")

    print("Correlation of highest and lowest temperature in Mustasaari:")
    thirdb = df_temp[df_temp['place']=='101464']
    correlationb4 = thirdb['lowest'].corr(thirdb['highest'])
    print(correlationb4)
    print("\n")

    print("Correlation of highest and lowest temperature in Pötsönvaara:")
    forthb = df_temp[df_temp['place']=='101649']
    correlationb5 = forthb['lowest'].corr(forthb['highest'])
    print(correlationb5)
    print("\n")




    # #Exercise 3 c)

    spsql = """
            SELECT AVG(airtemp) AS airtemp, latitude FROM Observation JOIN Place ON(Observation.place = Place.code) GROUP BY(place)
            """
    df = pd.read_sql_query(spsql, sqlite_conn)
    correlationc = df['airtemp'].corr(df['latitude'])
    print("Correlation between average air temperature and latitude over all places:")
    print(correlationc)
    print("\n")
    


    #Exercise 3 d)
    sqld = """
          SELECT * FROM (SELECT strftime('%m', date) AS month, COUNT(date) AS daysH FROM Observation WHERE place = "100968" AND rain>0 GROUP BY(strftime('%m', date)))
          LEFT OUTER JOIN
          (SELECT strftime('%m', date) AS month, COUNT(date) AS daysU FROM Observation WHERE place = "102035" AND rain>0 GROUP BY(strftime('%m', date))) USING(month)
          LEFT OUTER JOIN
          (SELECT strftime('%m', date) AS month, COUNT(date) AS daysM FROM Observation WHERE place = "101464" AND rain>0 GROUP BY(strftime('%m', date))) USING(month)
          LEFT OUTER JOIN
          (SELECT strftime('%m', date) AS month, COUNT(date) AS daysP FROM Observation WHERE place = "101649" AND rain>0 GROUP BY(strftime('%m', date))) USING(month)
        """
    testd = pd.read_sql_query(sqld, sqlite_conn)
    testd[['month','daysH']].groupby('month')['daysH'].sum().plot(kind='bar', title='Number of Rainy Days during each month in Helsinki-Vantaa Airport', xlabel='Month', ylabel='Rainy Days')
    plt.show()
    testd[['month','daysU']].groupby('month')['daysU'].sum().plot(kind='bar', title='Number of Rainy Days during each month in Utsjoki', xlabel='Month', ylabel='Rainy Days')
    plt.show()
    testd[['month','daysM']].groupby('month')['daysM'].sum().plot(kind='bar', title='Number of Rainy Days during each month in Mustasaari', xlabel='Month', ylabel='Rainy Days')
    plt.show()
    testd[['month','daysP']].groupby('month')['daysP'].sum().plot(kind='bar', title='Number of Rainy Days during each month in Pötsönvaara', xlabel='Month', ylabel='Rainy Days')
    plt.show()
    

    #Exercise 3 e)
    sqle = """
            SELECT date, tempH, tempU, tempM, tempP FROM (SELECT date, airtemp AS tempH FROM Observation WHERE place = "100968" AND airtemp IS NOT NULL) 
            LEFT OUTER JOIN (SELECT date, airtemp AS tempU FROM Observation WHERE place = "102035" AND airtemp IS NOT NULL) USING(date)
            LEFT OUTER JOIN (SELECT date, airtemp AS tempM FROM Observation WHERE place = "101464" AND airtemp IS NOT NULL) USING(date)
            LEFT OUTER JOIN (SELECT date, airtemp AS tempP FROM Observation WHERE place = "101649" AND airtemp IS NOT NULL) USING(date)
            """
    teste = pd.read_sql_query(sqle, sqlite_conn)
    plt.scatter(teste['date'], teste['tempH'], color='b', label='Helsinki-Vantaa Airport')
    plt.scatter(teste['date'], teste['tempU'], color='g', label='Utsjoki')
    plt.scatter(teste['date'], teste['tempM'], color='y', label='Mustasaari')
    plt.scatter(teste['date'], teste['tempP'], color='r', label='Pötsönvaara')
    plt.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
    plt.ylabel("Average Air Temperature")
    plt.title("Avegare Air Temperature throughout year 2020")
    plt.legend()
    plt.show()
    
except Exception as e:
    print ("FAILED due to:" + str(e))              
# END     
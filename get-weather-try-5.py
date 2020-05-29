import requests
import pandas as pd
from pandas import DataFrame
import numpy as np
from datetime import datetime
from os import path
import json


#Not sure why this didn't work...want to find out
#for i in range(2014, 2020):
#    df_{i} = pd.read_html('https://www.pro-football-reference.com/years/{i}/games.htm')
#    df_{i} = df_i[0]
    


#First we need to grab the various datasets with a bit of formatting
df_2019 = pd.read_html('https://www.pro-football-reference.com/years/2019/games.htm')
df_2019 = df_2019[0]
df_2019 = df_2019[df_2019.Date != 'Playoffs']
df_2019['Year'] = df_2019['Date'].str.contains(pat=r'^(January|February)', regex =True)
df_2019['Year'] = df_2019['Year'].astype(int)
df_2019['Year'] = np.where(df_2019['Year'], ', 2020', ', 2019')
df_2019['Date'] = df_2019['Date'] + df_2019['Year']

df_2018 = pd.read_html('https://www.pro-football-reference.com/years/2018/games.htm')
df_2018 = df_2018[0]
df_2018 = df_2018[df_2018.Date != 'Playoffs']
df_2018['Year'] = df_2018['Date'].str.contains(pat=r'^(January|February)', regex =True)
df_2018['Year'] = df_2018['Year'].astype(int)
df_2018['Year'] = np.where(df_2018['Year'], ', 2019', ', 2018')
df_2018['Date'] = df_2018['Date'] + df_2018['Year']

df_2017 = pd.read_html('https://www.pro-football-reference.com/years/2017/games.htm')
df_2017 = df_2017[0]
df_2017 = df_2017[df_2017.Date != 'Playoffs']
df_2017['Year'] = df_2017['Date'].str.contains(pat=r'^(January|February)', regex =True)
df_2017['Year'] = df_2017['Year'].astype(int)
df_2017['Year'] = np.where(df_2017['Year'], ', 2018', ', 2017')
df_2017['Date'] = df_2017['Date'] + df_2017['Year']

df_2016 = pd.read_html('https://www.pro-football-reference.com/years/2016/games.htm')
df_2016 = df_2016[0]
df_2016 = df_2016[df_2016.Date != 'Playoffs']
df_2016['Year'] = df_2016['Date'].str.contains(pat=r'^(January|February)', regex =True)
df_2016['Year'] = df_2016['Year'].astype(int)
df_2016['Year'] = np.where(df_2016['Year'], ', 2017', ', 2016')
df_2016['Date'] = df_2016['Date'] + df_2016['Year']

df_2015 = pd.read_html('https://www.pro-football-reference.com/years/2015/games.htm')
df_2015 = df_2015[0]
df_2015 = df_2015[df_2015.Date != 'Playoffs']
df_2015['Year'] = df_2015['Date'].str.contains(pat=r'^(January|February)', regex =True)
df_2015['Year'] = df_2015['Year'].astype(int)
df_2015['Year'] = np.where(df_2015['Year'], ', 2016', ', 2015')
df_2015['Date'] = df_2015['Date'] + df_2015['Year']

df_2014 = pd.read_html('https://www.pro-football-reference.com/years/2014/games.htm')
df_2014 = df_2014[0]
df_2014 = df_2014[df_2014.Date != 'Playoffs']
df_2014['Year'] = df_2014['Date'].str.contains(pat=r'^(January|February)', regex =True)
df_2014['Year'] = df_2014['Year'].astype(int)
df_2014['Year'] = np.where(df_2014['Year'], ', 2015', ', 2014')
df_2014['Date'] = df_2014['Date'] + df_2014['Year']

df_2013 = pd.read_html('https://www.pro-football-reference.com/years/2013/games.htm')
df_2013 = df_2013[0]
df_2013 = df_2013[df_2013.Date != 'Playoffs']
df_2013['Year'] = df_2013['Date'].str.contains(pat=r'^(January|February)', regex =True)
df_2013['Year'] = df_2013['Year'].astype(int)
df_2013['Year'] = np.where(df_2013['Year'], ', 2014', ', 2013')
df_2013['Date'] = df_2013['Date'] + df_2013['Year']


#Combine all the individual datasets and drop extra header rows
all_frames = [df_2013, df_2014, df_2015, df_2016, df_2017, df_2018, df_2019]
master_data = pd.concat(all_frames)
master_data = master_data[master_data.Week != 'Week']
master_data = master_data[master_data.Date != 'Playoffs']
master_data = master_data.reset_index(drop=True)


#Is there an easier way to do this?
master_data['PtsW'] = master_data['PtsW'].astype(int)
master_data['PtsL'] = master_data['PtsL'].astype(int)
master_data['YdsW'] = master_data['YdsW'].astype(int)
master_data['YdsL'] = master_data['YdsL'].astype(int)
master_data['TOW'] = master_data['TOW'].astype(int)
master_data['TOL'] = master_data['TOL'].astype(int)

#------------------------Additional formatting
#Name unnamed columns
master_data.rename(columns={'Unnamed: 5':'At', 'Unnamed: 7':'Boxscore'}, inplace=True)
master_data = master_data.drop('Boxscore', axis=1)
master_data.rename(columns={'Date':'Long_Date'}, inplace=True)
#Fix Date Column 
master_data['Short_Date'] = pd.to_datetime(master_data['Long_Date']).dt.date
#Fix Time Column
master_data['Long_Time'] = pd.to_datetime(master_data['Time']).dt.time
master_data['Timestamp'] = master_data.apply(lambda x : datetime.combine(x['Short_Date'],x['Long_Time']), 1)


#Get Total Game Points
master_data['Total_Pts'] = master_data['PtsW'] + master_data['PtsL']

#Get Home Team
master_data.loc[master_data['At'] == '@', 'Home_Team'] = master_data['Loser/tie']
master_data['Home_Team'].fillna(value=master_data['Winner/tie'], inplace=True)

#Load in Stadium Information
DATA_DIR = 'C:/Users/xbsqu/Desktop/Python Learning/Projects/Football Model'
stadiums = pd.read_csv(path.join(DATA_DIR, 'stadium-info.csv'))
stadiums.rename(columns={'Team':'Home_Team'}, inplace=True)

#Join the tables
master_data = master_data.merge(stadiums, on='Home_Team').sort_values('Timestamp')
#Formatting
master_data = master_data.reset_index(drop=True)
master_data = master_data.drop('Year', axis=1)
#Need to convert Short_Date from dt to str
master_data['Short_Date'] = master_data['Short_Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
#Get just the outdoor games
outdoor_games =  master_data[master_data.Roof_Type == 'Open']

#Using a Sample Test group tp preserve API calls for when I inevitably fuck everything up
test_outdoor = outdoor_games.sample(n=10)


#-------------------------Step 2: Weather


station = test_outdoor['Weather_Station']
date = test_outdoor['Short_Date']
api_key = 'obcnO9Ye'



def get_weather():
        for game in test_outdoor:
        
            api_url = 'https://api.meteostat.net/v1/history/daily?'
            full_url = api_url + 'station=' + station + '&start=' + date + '&end=' + date + '&key=' + api_key
            response = requests.get(full_url)
            json_data = response.json()
            data = json.load(json_data)
            
            print(data)
        
what_is_this = get_weather()


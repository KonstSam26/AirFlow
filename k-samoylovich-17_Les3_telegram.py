import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram
import json
from urllib.parse import urlencode
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable



login = 'k-samojlovich-17'
year = 1994 + hash(f'{login}') % 23
path_to_file = '/var/lib/airflow/airflow.git/dags/k-samojlovich-17/vgsales.csv'

default_args = {
    'owner': 'k-samojlovich',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 3, 10),
    'schedule_interval': '00 10 * * *'
}

CHAT_ID = -657696843
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
#         bot = telegram.Bot(token=BOT_TOKEN)
#         bot.send_message(chat_id=CHAT_ID, message=message)
        message = f'Huge success! Dag {dag_id} completed on {date}'
        params = {'chat_id' : CHAT_ID, 'text' : message}
        base_url = f'https://api.telegram.org/bot{BOT_TOKEN}/'
        url = base_url + 'sendMessage?' + urlencode(params)
        resp = requests.get(url)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def k_samojlovich_les3_teleg():
    @task(retries=3, retry_delay=timedelta(10))
    def k_samojlovich_get_data():
        data = pd.read_csv(path_to_file)
        data = data.query('Year == @year')
        return data


    @task()
    def k_samojlovich_top_game(data):
        top_game = data.sort_values('Global_Sales', ascending=False) \
                       .iloc[0]['Name']
        return top_game


    @task()
    def k_samojlovich_top_genre(data):
        top_EU_genre_max = data.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})\
                               .sort_values('EU_Sales', ascending=False).EU_Sales.iloc[0] 
        top_EU_genre = data.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})\
                           .query('EU_Sales == @top_EU_genre_max').Genre.to_list()
        return top_EU_genre


    @task()
    def k_samojlovich_top_NA_platform(data):
        top_NA_platform_1 = data.query('NA_Sales > 1').groupby('Platform', as_index=False).agg({'NA_Sales': 'count'})
        top_NA_platform_max = top_NA_platform_1.sort_values('NA_Sales', ascending=False).NA_Sales.iloc[0] 
        top_NA_platform = top_NA_platform_1.query('NA_Sales == @top_NA_platform_max').NA_Sales.to_list()            
        return top_NA_platform

    @task()
    def k_samojlovich_top_JP_publisher(data):
        top_JP_sales = data.groupby('Publisher', as_index=False)\
                           .agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False)
        top_JP_sales_max = top_JP_sales.JP_Sales.iloc[0] 
        top_JP_avg_sales = top_JP_sales.query('JP_Sales == @top_JP_sales_max').Publisher.to_list()
        return top_JP_avg_sales

    
    
    
    @task()
    def k_samojlovich_eu_jp_sales(data):
        eu_jp_sales = data.query('EU_Sales > JP_Sales').Name.nunique()
        return eu_jp_sales


    @task(on_success_callback=send_message)
    def k_samojlovich_print_data(top_game, top_EU_genre, top_NA_platform, top_JP_publisher, eu_jp_sales):

        context = get_current_context()
        date = context['ds']

        print(f'''Game sales data in {year} for {date}:
        TOP game in Global sales:
            {top_game}
        TOP genre in Europe:
            {top_EU_genre}
        TOP platform in NA > 1 million game copies:
            {top_NA_platform}
        TOP Publisher Average Game Sales in Japan:
            {top_JP_publisher}
        How many games were sold better in EU than in JP:
            {eu_jp_sales}''')
        

    data = k_samojlovich_get_data()
    top_game = k_samojlovich_top_game(data)
    top_EU_genre = k_samojlovich_top_genre(data)
    top_NA_platform = k_samojlovich_top_NA_platform(data)
    top_JP_publisher = k_samojlovich_top_JP_publisher(data)
    eu_jp_sales = k_samojlovich_eu_jp_sales(data)
    k_samojlovich_print_data(top_game, top_EU_genre, top_NA_platform, top_JP_publisher, eu_jp_sales)
        
k_samojlovich_les3_teleg = k_samojlovich_les3_teleg() # запуск дага


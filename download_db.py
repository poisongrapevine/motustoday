# Downloads the database
# Can be used to refresh the database automatically

import pandas as pd
import numpy as np
import sqlite3
import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import os

import geopandas
from shapely.ops import unary_union
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

def get_page(url): #скачивает страницу в soup
    
    user_agent = UserAgent().chrome
    try:
        response = session.get(url, headers={'User-Agent':user_agent})
    except Exception as err:
        with open('errors.txt', 'a') as f:
            f.write(f'{err}\t{url}\n')
    #response = session.get(url, proxies=proxy)
    soup = BeautifulSoup(response.text, 'html.parser')
    time.sleep(random.random())
    
    return soup

def to_sqlite(file, folder='tables/', stops=[]):
    if file not in stops:
        df = pd.read_csv(os.path.join(folder, file), engine='python')
        df.to_sql(file, con, if_exists='replace', index=False)
        
def csv_loader(url, name, base='https://motus.org', folder='tables/'):
    url = base + url
    r = requests.get(url, allow_redirects=True)
    open(folder + name + '.csv', 'wb').write(r.content)
    
def continent(gps):
    point = Point(reversed(gps))
    if continents.loc[continents['geometry'].contains(point)].empty:
        return ''
    else:
        return continents.loc[continents['geometry'].contains(point)]['continent'].values[0]

def download_db():
    
    down = get_page('https://motus.org/data/downloads')

    down_links = []

    for ele in down.find_all('a'):
        if 'api-proxy' in ele['href']:
            down_links.append(ele['href'])

            names = dict(zip(down_links, [
        'motus_projects',
        'tags',
        'tag_deployments',
        'receiver_deployments',
        'antenna_deployments',
        'receivers',
        'fields',
        'species',
        'active_tags'
    ]))

    for url, name in names.items():
        csv_loader(url, name)

    # replace a sql-unfriendly column name

    with open('tables/species.csv', 'r') as f:
        content = f.read()

    content = content.replace('group', 'group__')

    with open('tables/species.csv', 'w') as f:
        f.write(content)

    # put the continent column in receiver_deployments

    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    continents = world.groupby('continent')['geometry'].apply(unary_union).reset_index()

    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    continents = world.groupby('continent')['geometry'].apply(unary_union).reset_index()

    rd = pd.read_csv('tables/receiver_deployments.csv')
    rd['args'] = rd[['latitude', 'longitude']].values.tolist()
    rd['continent'] = rd['args'].apply(continent)
    rd.drop(columns=['args'], inplace=True)
    rd.to_csv('tables/receiver_deployments.csv')

    sp = pd.read_csv('tables/species.csv')
    sp['group__'] = sp['group__'].map({"BATS": 'Mammals',
        "MAMMALS": 'Mammals',
        "BEETLES": 'Insects',
        "BUTTERFL": 'Insects',
        "HYMENOPTERA": 'Insects',
        "MOTHS": 'Insects',
        "ODONATA": 'Insects',
        "ORTHOPTERA": 'Insects',
        "BIRDS": 'Birds or reptiles',
        "REPTILES": 'Birds or reptiles'})
    sp.to_csv('tables/species.csv')

    con = sqlite3.connect('../motus.db') 
    cur = con.cursor() 

    for file in os.listdir('tables/'):
        if file.endswith('.csv'):
            to_sqlite(file)
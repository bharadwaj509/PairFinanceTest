import asyncio
import json
from os import environ
from time import sleep, time
from geopy import distance
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy import Table, Column, Integer, String, MetaData, func

print('Waiting for the data generator...')
# sleep(20)
print('ETL Starting...')

POSTGRESQL_CS = 'postgresql+psycopg2://postgres:password@psql_db:5432/main'
MYSQL_CS = 'mysql+pymysql://nonroot:nonroot@mysql_db/analytics?charset=utf8'

# environ["POSTGRESQL_CS"]
# environ["MYSQL_CS"]

while True:
    try:
        psql_engine = create_engine(POSTGRESQL_CS, pool_pre_ping=True, pool_size=10)
        mysql_engine = create_engine(MYSQL_CS, pool_pre_ping=True, pool_size=10)
        metadata_obj = MetaData()
        devices_mysql = Table(
            'device_aggregations', metadata_obj,
            Column('device_id', String),
            Column('max_temperature', Integer),
            Column('distace_location', Integer),
            Column('number_of_points', Integer),
        )
        metadata_obj.create_all(psql_engine)
        break
        break
    except OperationalError:
        sleep(0.1)

print('Connection to PostgresSQL successful, and going to MySql.')

metadata_obj = MetaData()
devices = Table(
        'devices', metadata_obj,
        Column('device_id', String),
        Column('temperature', Integer),
        Column('location', String),
        Column('time', String),
    )

def storeDataPointMysql(data_points):
    for data_point in data_points:
        ins = devices_mysql.insert()  
        with mysql_engine.connect() as conn:
            while True:            
                data = dict(
                    device_id=data_point[0],
                    max_temperature=data_point[1],
                    distace_location=data_point[2],
                    number_of_points=data_point[3]
                )            
                conn.execute(ins, data)
                conn.commit()
                print(device_id, data['time']) 


def Sort(sub_li):
    sub_li.sort(key = lambda x: x[0])
    return sub_li

def getUniqueIds(sorted_results):
    return list(set([res[0] for res in sorted_results]))

def getListsById(unique_ids, sorted_results):
    sub_lists = []
    for id in unique_ids:
        ls = [x for x in sorted_results if x[0]==id]
        sub_lists.append(ls)
    return sub_lists

def getMax(ls):
    max_temp = 0
    for item in ls:
        if item[1] > max_temp:
            max_temp = item[1]
    return max_temp

def getDist(ls):
    dist = 0
    for i in range(1,len(ls)-1):
        location1 = (json.loads(ls[i-1][2])['latitude'], json.loads(ls[i-1][2])['longitude'])
        location2 = (json.loads(ls[i][2])['latitude'], json.loads(ls[i][2])['longitude'])
        # print(json.loads(ls[i-1][2])['latitude'])
        
        dist = distance.distance(location1, location2).miles + dist

    return dist

def getParams(sub_lists_by_id):
    results = []
    for ls in sub_lists_by_id:
        max_temperature = getMax(ls)
        number_of_items = len(ls)
        total_distance = getDist(ls)
        print(ls[0], max_temperature, total_distance, number_of_items)
        results.append((ls[0], max_temperature, total_distance, number_of_items))

def get_data_point():
    
    with psql_engine.connect() as conn:
        while True:            
            
            one_hour = 60*60 # 3600 seconds
            
            hour_count = one_hour * 3 # since there are 3 records inserted every second, there will be 3600 * 3 times the records per hour
            sleep(10) # sleep time would be one hour as we are looking for max_temperature, distance of the device and number of records per hour

            query = devices.select().order_by(devices.c.time.desc()).limit(hour_count) # querying the data for every hour in descending order by time so that we will get the latest records
            print("Query is ", query)     
            results = conn.execute(query).fetchall()
            
            print("results is ", len(results))
            
            sorted_results = Sort(results) # sorting the results by the device id, by grouping all the rows together
            
            unique_ids = getUniqueIds(sorted_results) # getting the unique ids 
            
            sub_lists_by_id = getListsById(unique_ids, sorted_results) # grouping the lists by unique ids
            
            params = getParams(sub_lists_by_id) # gets max_temperature, total_distance, number_of_items for the device for that hour
            
            storeDataPointMysql(params) # stores the data into mysql database
            
            print("**************************")


get_data_point()
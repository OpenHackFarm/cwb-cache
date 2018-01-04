# -*- coding: utf-8 -*-

# 中央氣象局氣象資料開放平臺 – 資料擷取使用說明
# http://opendata.cwb.gov.tw/opendatadoc/CWB_Opendata_API_V1.2.pdf

import requests
import ConfigParser
from influxdb import InfluxDBClient
import dateutil
import datetime
from config import *
import os

LAST_UPDATE_CACHE_FILE = '_last_update_time_'

dirname, filename = os.path.split(os.path.abspath(__file__))

config = ConfigParser.RawConfigParser()

inClient = InfluxDBClient(host=INFLUXDB_HOST, database=INFLUXDB_DATABASE)

type_mapping = {
    'O_A0001_001': {
        "ELEV": 'float',
        "WDIR": 'int',
        "WDSD": 'float',
        "TEMP": 'float',
        "HUMD": 'float',
        "PRES": 'float',
        "SUN": 'float',
        "H_24R": 'float',
        "H_FX": 'float',
        "H_XD": 'int',
        "H_FXT": 'int'
    },
    'O_A0002_001': {
        'ELEV': 'float',
        'RAIN': 'float',
        'MIN_10': 'float',
        'HOUR_3': 'float',
        'HOUR_6': 'float',
        'HOUR_12': 'float',
        'HOUR_24': 'float',
        'NOW': 'float'
    },
    'O_A0003_001': {
        'ELEV': 'float',
        'WDIR': 'int',
        'WDSD': 'float',
        'TEMP': 'float',
        'HUMD': 'float',
        'PRES': 'float',
        '24R': 'float',
        'H_FX': 'float',
        'H_XD': 'int',
        'H_FXT': 'int',
        'H_F10': 'float',
        'H_10D': 'int',
        'H_F10T': 'int',
        'H_UVI': 'int',
        'D_TX': 'float',
        'D_TXT': 'int',
        'D_TS': 'float'
    }
}


def convert_int(v):
    try:
        f = int(v)
    except:
        if 'X' in v:
            f = -998
        else:
            f = -999

    return f


def convert_float(v):
    try:
        f = float(v)
    except:
        if 'X' in v:
            f = -998.0
        else:
            f = -999.0

    return f


def convert_datetime(dt):
    old_datetime = dateutil.parser.parse(dt)
    new_datetime = old_datetime + datetime.timedelta(hours=-8)
    return new_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')  # RFC3339


def fetch_dataset(dataset):
    if not config.has_section(dataset):
        config.add_section(dataset)

    r = requests.get('https://opendata.cwb.gov.tw/api/v1/rest/datastore/' +
                     dataset, headers={"Authorization": CWB_AUTHORIZATION})

    # print dir(r)
    # print r.json().keys()
    # print r.json()['records']
    # print r.json()['result']
    # print r.json()['success']

    # print type(r.json()['records']['location'])
    # print r.json()['records']['location'][0].keys()
    # print r.json()['records']['location'][0]

    print(r.text)

    for l in r.json()['records']['location']:
        if not config.has_option(dataset, l['stationId']):
            config.set(dataset, l['stationId'], '')

        if l['time']['obsTime'] != config.get(dataset, l['stationId']):
            config.set(dataset, l['stationId'], l['time']['obsTime'])

            print l['stationId'], l['locationName']

            # create tags
            tags = {}
            tags['id'] = l['stationId']
            tags['name'] = l['locationName']
            tags['latitude'] = l['lat']
            tags['longitude'] = l['lon']
            for p in l['parameter']:
                tags[p['parameterName']] = p['parameterValue']

            # create fields
            fields = {}
            convert_map = {
                'int': convert_int,
                'float': convert_float,
                'datetime': convert_datetime,
            }
            for w in l['weatherElement']:
                t = type_mapping[dataset.replace('-', '_')][w['elementName']]
                fields[w['elementName']] = convert_map[t](w['elementValue'])
            print fields

            # convert to UTC timezone
            dt = convert_datetime(l['time']['obsTime'])
            print dt

            upload_to_influxdb(INFLUXDB_MEASUREMENT, tags, fields, dt)


def upload_to_influxdb(measurement, tags, fields, time=None):
    data = {
        "measurement": measurement,
        "tags": tags,
        "fields": fields
    }
    if time:
        data['time'] = time

    # inClient.write_points([data])


if __name__ == '__main__':
    config.read(LAST_UPDATE_CACHE_FILE)

    fetch_dataset('O-A0001-001')  # 自動氣象站-氣象觀測資料
    fetch_dataset('O-A0002-001')  # 自動雨量站-雨量觀測資料
    fetch_dataset('O-A0003-001')  # 局屬氣象站-現在天氣觀測報告

    with open(os.path.join(dirname, LAST_UPDATE_CACHE_FILE), 'wb') as configfile:
        config.write(configfile)

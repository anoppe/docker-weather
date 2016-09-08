#! /usr/bin/env python

from influxdb import InfluxDBClient
import json
import os
import subprocess
import sys
import time
import urllib
import urllib2

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def record_weather(api_key, latitude, longitude, db_addr, db_port, db_name, period):
    url = "https://api.forecast.io/forecast/%s/%s?units=si&exclude=minutely,hourly,daily,alerts,flags" % (api_key, ",".join([latitude, longitude]))
    client = InfluxDBClient(db_addr, db_port, 'root', 'root', db_name)

    while True:
        try:
            result = urllib2.urlopen(url).read()
            data = json.loads(result)
        except Exception as ex:
            print "Tried url: " + url
            raise ex

        database_dicts = client.get_list_database()
        for db in database_dicts:
            if(db['name'] == db_name):
                break
        else:
            client.create_database(db_name)

        json_body = [{
            "measurement" : key,
            "fields": {
                "value": float(value)
            }} for key, value in data['currently'].items() if isfloat(value)]

        print "Sending to InfluxDB: \n" + str(json_body)
        print "Result: "
        print client.write_points(json_body)

        print "Complete"
        print ""

        time.sleep(period)


def main():
    # Required
    api_key = os.environ.get("API_KEY")
    latitude = os.environ.get("LATITUDE")
    longitude = os.environ.get("LONGITUDE")

    # Optional
    db_addr = os.getenv("INFLUXDB_ADDRESS", 'influxdb')
    db_port = os.getenv("INFLUXDB_PORT", 8086)
    db_name = os.getenv("INFLUXDB_NAME", 'weather')
    period = int(os.getenv("PERIOD", 120))

    print "Entering main loop..."
    record_weather(api_key, latitude, longitude, db_addr, db_port, db_name, period)


if __name__ == "__main__":
    main()


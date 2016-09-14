#! /usr/bin/env python

from __future__ import print_function
from influxdb import InfluxDBClient
import json
import os
import signal
import subprocess
import sys
import time
import urllib
import urllib2

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        print("Exiting gracefully")
        self.kill_now = True

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def record_weather(api_key, latitude, longitude, db_addr, db_port, db_name, period, units, location, tags):
    killer = GracefulKiller()

    # Bulid URL for Forecast.io request
    url = "https://api.forecast.io/forecast/{api_key}/{latitude},{longitude}" \
            "?units={units}&exclude=minutely,hourly,daily,alerts,flags"
    url = url.format(api_key=api_key,
                     units=units,
                     latitude=latitude,
                     longitude=longitude)

    # Parse given tags, and add location
    json_acceptable_string = tags.replace("'", "\"")
    tags_set = json.loads(json_acceptable_string)
    tags_set['location'] = location

    # Establish connection with InfluxDB
    print("Establishing connection to InfluxDB database... ", end="")
    client = InfluxDBClient(db_addr, db_port, 'root', 'root', db_name)
    print("Done.")

    while not killer.kill_now:
        try:
            result = urllib2.urlopen(url).read()
            data = json.loads(result)
        except Exception as ex:
            print("Tried url: " + url)
            raise ex

        database_dicts = client.get_list_database()
        for db in database_dicts:
            if(db['name'] == db_name):
                break
        else:
            client.create_database(db_name)

        json_body = [{
            "measurement" : key,
            "tags" : tags_set,
            "fields": {
                "value": float(value)
            }} for key, value in data['currently'].items() if isfloat(value)]

        print("Sending to InfluxDB: \n" + str(json_body))
        print("Write success: ", end="")
        print(client.write_points(json_body))
        print("Measurement complete")
        print("Sleeping for %d seconds..." % period)
        print()
        sys.stdout.flush()

        # Sleep in short bursts, so that we may exit gracefully
        sleep_start = time.time()
        while time.time() - sleep_start < period and not killer.kill_now:
            time.sleep(1)

def get_required_env(name):
    variable = os.environ.get(name)
    if not variable:
        print("Environment variable %s is required. Exiting." % name);
        quit(-1)
    return variable

def main():
    # Required
    api_key = get_required_env("API_KEY")
    latitude = get_required_env("LATITUDE")
    longitude = get_required_env("LONGITUDE")
    location = get_required_env("LOCATION")

    # Optional
    db_addr = os.getenv("INFLUXDB_ADDRESS", 'influxdb')
    db_port = os.getenv("INFLUXDB_PORT", 8086)
    db_name = os.getenv("INFLUXDB_NAME", 'weather')
    period = int(os.getenv("PERIOD", 120))
    units = os.getenv("UNITS", "us")
    tags = os.getenv("TAGS", "{}")

    print("Entering main loop...")
    record_weather(api_key, latitude, longitude, db_addr, db_port, db_name, period, units, location, tags)


if __name__ == "__main__":
    main()


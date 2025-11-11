#!/usr/bin/env python3
import psycopg2
from datetime import datetime
import time
import random
import argparse
import re
import logging
import sys
import requests
import os
from btlewrap import available_backends, BluepyBackend, GatttoolBackend, PygattBackend
from mitemp_bt.mitemp_bt_poller import MiTempBtPoller, MI_TEMPERATURE, MI_HUMIDITY, MI_BATTERY
from syslog import syslog
import config
 
 
capteurs={
     '1':{
         'mac': '58:2d:34:32:60:33',
     },
    '2':{
        'mac': '58:2d:34:32:64:ad',
    },
    '3':{
        'mac': '58:2d:34:32:5c:cf',
    },
     '4':{
         'mac': '58:2d:34:32:64:ab',
     },
}
 

if len(sys.argv)!=2:
    print(u'Paramètres attendus : ')
    print(u'- Capteur : 1, 2, 3 ou 4')
    exit()
parametre = sys.argv[1]
 
 
# Connexion à TimescaleDB
connection_string = (
    f"host={config.TIMESCALEDB_HOST} "
    f"port={config.TIMESCALEDB_PORT} "
    f"user={config.TIMESCALEDB_USER} "
    f"password={config.TIMESCALEDB_PASSWORD} "
    f"dbname={config.TIMESCALEDB_DATABASE} "
    f"sslmode={config.TIMESCALEDB_SSLMODE}"
)
conn = psycopg2.connect(connection_string)
 
 
def write_timescaledb(mac, capteur, measurement, value):
    """Écrit les données dans TimescaleDB"""
    try:
        cursor = conn.cursor()
        timestamp = datetime.now()
        
        # Insertion dans TimescaleDB
        # Adapter le nom de la table selon votre schéma
        query = """
            INSERT INTO sensor_data (time, mac, capteur, measurement, value)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (timestamp, mac, capteur, measurement, value))
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        print(f"Erreur lors de l'écriture dans TimescaleDB: {e}")
        conn.rollback()
        return False
 
 
def get_value(poller,MI):
    value=False
    try:
        res = poller.parameter_value(MI),
        if res:
            value=res[0]
    except:
        pass
    return value
 
 
for line in capteurs:
    if line==parametre:
        syslog(str(line)+u'...')
        capteur = capteurs[line]
        if 'mac' in capteur:
            mac = capteur['mac']
            poller = False
            try:
                poller = MiTempBtPoller(mac, BluepyBackend)
            except:
                print(line, mac, "Problème de communication avec le capteur")
                

                
            if poller:
                temperature = get_value(poller,MI_TEMPERATURE)
                if temperature:
                    write_timescaledb(mac, line, 'MI_TEMPERATURE', temperature)
                humidity = get_value(poller,MI_HUMIDITY)
                if humidity:
                    write_timescaledb(mac, line, 'MI_HUMIDITY', humidity)
                battery = get_value(poller,MI_BATTERY)
                if battery:
                    write_timescaledb(mac, line, 'MI_BATTERY', battery)
                    
                print(mac,poller,temperature,humidity,battery)
                    
                    
                syslog(str(line)+u' : '+str(mac)+u' : '+str(temperature)+u' : '+str(humidity)+u' : '+str(battery))

# Fermer la connexion à la fin
conn.close()



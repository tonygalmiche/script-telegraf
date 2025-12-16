#!/usr/bin/env python3
import psycopg2
from datetime import datetime
import board
import adafruit_dht
from syslog import syslog
import config


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


def write_timescaledb(measurement, value):
    """Écrit les données dans TimescaleDB"""
    try:
        cursor = conn.cursor()
        timestamp = datetime.now()
        
        # Insertion dans TimescaleDB
        # Adapter le nom de la table selon votre schéma
        query = """
            INSERT INTO sensor_data (time, measurement, value)
            VALUES (%s, %s, %s)
        """
        cursor.execute(query, (timestamp, measurement, value))
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        print(f"Erreur lors de l'écriture dans TimescaleDB: {e}")
        conn.rollback()
        return False


def main() -> None:
    try:
        dhtDevice = adafruit_dht.DHT22(board.D4)
        temperature = dhtDevice.temperature
        humidity = dhtDevice.humidity
        
        # Écriture dans TimescaleDB
        if temperature is not None:
            write_timescaledb('temperature', temperature)
            syslog(f'DHT22 - temperature: {temperature}°C')
            print(f'Temperature: {temperature}°C')
        
        if humidity is not None:
            write_timescaledb('humidity', humidity)
            syslog(f'DHT22 - humidity: {humidity}%')
            print(f'Humidity: {humidity}%')
        
        dhtDevice.exit()
    except Exception as error:
        print(f"Erreur: {error}")
        syslog(f"Erreur DHT22: {error}")
        raise error
    finally:
        # Fermer la connexion
        conn.close()


if __name__ == '__main__':
    main()

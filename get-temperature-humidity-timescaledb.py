#!/usr/bin/env python3
import psycopg2
from datetime import datetime
import board
import adafruit_dht
from syslog import syslog
import config


def write_timescaledb(conn, measurement, value):
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
    conn = None
    dhtDevice = None
    
    try:
        # Lire les données du capteur d'abord
        print("Initialisation du capteur DHT22...")
        dhtDevice = adafruit_dht.DHT22(board.D4)
        
        print("Lecture de la température...")
        temperature = dhtDevice.temperature
        print(f'Temperature lue: {temperature}°C')
        
        print("Lecture de l'humidité...")
        humidity = dhtDevice.humidity
        print(f'Humidity lue: {humidity}%')
        
        # Connexion à TimescaleDB seulement si on a des données
        if temperature is not None or humidity is not None:
            print("Connexion à TimescaleDB...")
            connection_string = (
                f"host={config.TIMESCALEDB_HOST} "
                f"port={config.TIMESCALEDB_PORT} "
                f"user={config.TIMESCALEDB_USER} "
                f"password={config.TIMESCALEDB_PASSWORD} "
                f"dbname={config.TIMESCALEDB_DATABASE} "
                f"sslmode={config.TIMESCALEDB_SSLMODE}"
            )
            conn = psycopg2.connect(connection_string)
            print("Connecté à TimescaleDB")
            
            # Écriture dans TimescaleDB
            if temperature is not None:
                print(f"Écriture de la température ({temperature}°C) dans TimescaleDB...")
                write_timescaledb(conn, 'temperature', temperature)
                syslog(f'DHT22 - temperature: {temperature}°C')
                print("Température écrite avec succès")
            
            if humidity is not None:
                print(f"Écriture de l'humidité ({humidity}%) dans TimescaleDB...")
                write_timescaledb(conn, 'humidity', humidity)
                syslog(f'DHT22 - humidity: {humidity}%')
                print("Humidité écrite avec succès")
        else:
            print("Aucune donnée valide à écrire dans TimescaleDB")
        
    except Exception as error:
        print(f"ERREUR: {error}")
        print(f"Type d'erreur: {type(error).__name__}")
        syslog(f"Erreur DHT22: {error}")
        raise error
    finally:
        # Fermer les ressources
        print("Fermeture des ressources...")
        if dhtDevice:
            print("Fermeture du capteur DHT22...")
            dhtDevice.exit()
        if conn:
            print("Fermeture de la connexion TimescaleDB...")
            conn.close()
        print("Script terminé")


if __name__ == '__main__':
    main()

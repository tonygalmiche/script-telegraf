#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script pour lire les capteurs de température Xiaomi Mijia LYWSD03MMC via Zigbee2MQTT
Les capteurs ont été flashés pour fonctionner en Zigbee
Envoie les données dans TimescaleDB
"""

import sys
import json
from datetime import datetime
import psycopg2
import config
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
import time

class Zigbee2MQTTHandler:
    """Gestionnaire MQTT pour capteurs Zigbee"""
    
    def __init__(self, sensors_dict, db_conn=None):
        self.sensors_dict = sensors_dict
        self.db_conn = db_conn
        self.devices_data = {}
        self.client = None
        self.start_time = None
        self.message_count = 0
        
    def on_connect(self, client, userdata, flags, rc):
        """Callback appelé lors de la connexion au broker MQTT"""
        if rc == 0:
            print("✓ Connecté au broker MQTT\n")
            self.start_time = time.time()
            # S'abonner aux topics des capteurs
            for sensor_id in self.sensors_dict.keys():
                topic = f"{config.MQTT_BASE_TOPIC}/{sensor_id}"
                client.subscribe(topic)
                print(f"📡 Abonné à: {topic}")
            print()
        else:
            print(f"✗ Erreur connexion MQTT: code {rc}")
    
    def on_message(self, client, userdata, msg):
        """Callback appelé lors de la réception d'un message MQTT"""
        try:
            self.message_count += 1
            elapsed = int(time.time() - self.start_time) if self.start_time else 0
            
            # Parser le payload JSON
            payload = json.loads(msg.payload.decode())
            
            # Extraire l'ID du capteur depuis le topic
            sensor_id = msg.topic.split('/')[-1]
            
            # Vérifier que c'est un de nos capteurs
            if sensor_id not in self.sensors_dict:
                return
            
            # Extraire les données
            temperature = payload.get('temperature')
            humidity = payload.get('humidity')
            battery = payload.get('battery')
            voltage = payload.get('voltage')
            linkquality = payload.get('linkquality')
            
            # Extraire les informations de mise à jour (pour affichage uniquement)
            update_info = payload.get('update', {})
            installed_version = update_info.get('installed_version')
            latest_version = update_info.get('latest_version')
            
            if temperature is None or humidity is None:
                return
            
            # Récupérer le nom du capteur
            name = self.sensors_dict[sensor_id]
            
            # Heure actuelle en timezone Paris
            now_paris = datetime.now(ZoneInfo("Europe/Paris"))
            time_str = now_paris.strftime("%Y-%m-%d %H:%M")
            
            # Affichage sur une seule ligne
            battery_str = f"{battery:5.1f}%" if battery is not None else "  N/A"
            voltage_str = f"({voltage} mV)" if voltage is not None else ""
            linkq_str = f"{linkquality:3d}" if linkquality is not None else "N/A"
            
            # Versions firmware (affichage uniquement)
            version_str = ""
            if installed_version and latest_version:
                version_str = f"  📦 v{installed_version}"
                if installed_version < latest_version:
                    version_str += f" ⚠️ MAJ dispo: v{latest_version}"
            
            print(f"{time_str}  {name}  {sensor_id}  🌡️ {temperature:5.1f}°C  💧 {humidity:5.1f}%  🔋 {battery_str} {voltage_str}  📡 {linkq_str}{version_str}")
            
            # Stockage
            self.devices_data[sensor_id] = {
                'name': name,
                'temperature': temperature,
                'humidity': humidity,
                'battery': battery,
                'voltage': voltage,
                'linkquality': linkquality,
                'timestamp': datetime.now()
            }
            
            # Écriture dans TimescaleDB
            if self.db_conn:
                self.write_timescaledb(sensor_id, name, temperature, humidity, battery, voltage, linkquality)
            
        except Exception as e:
            print(f"✗ Erreur traitement message: {e}")
    
    def write_timescaledb(self, mac, capteur, temperature, humidity, battery, voltage, linkquality):
        """Écrit les données dans TimescaleDB"""
        try:
            cursor = self.db_conn.cursor()
            timestamp = datetime.now()
            
            # Insertion température
            query = """
                INSERT INTO sensor_data (time, mac, capteur, measurement, value)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, (timestamp, mac, capteur, 'MI_TEMPERATURE', temperature))
            
            # Insertion humidité
            cursor.execute(query, (timestamp, mac, capteur, 'MI_HUMIDITY', humidity))
            
            # Insertion batterie (%)
            if battery is not None:
                cursor.execute(query, (timestamp, mac, capteur, 'MI_BATTERY', battery))
            
            # Insertion voltage batterie (mV)
            if voltage is not None:
                cursor.execute(query, (timestamp, mac, capteur, 'MI_BATTERY_MV', voltage))
            
            # Insertion qualité du lien Zigbee
            if linkquality is not None:
                cursor.execute(query, (timestamp, mac, capteur, 'MI_LINKQUALITY', linkquality))
            
            self.db_conn.commit()
            cursor.close()
        except Exception as e:
            print(f"✗ Erreur TimescaleDB pour {capteur}: {e}")
            self.db_conn.rollback()


def listen_mqtt(sensors_dict, db_conn=None, duration=None):
    """Écoute les messages MQTT des capteurs Zigbee"""
    
    handler = Zigbee2MQTTHandler(sensors_dict, db_conn)
    
    # Créer le client MQTT
    client = mqtt.Client()
    handler.client = client
    
    # Configurer les callbacks
    client.on_connect = handler.on_connect
    client.on_message = handler.on_message
    
    # Authentification si configurée
    if config.MQTT_USERNAME and config.MQTT_PASSWORD:
        client.username_pw_set(config.MQTT_USERNAME, config.MQTT_PASSWORD)
    
    try:
        # Connexion au broker
        print(f"🔌 Connexion au broker MQTT {config.MQTT_BROKER}:{config.MQTT_PORT}...")
        client.connect(config.MQTT_BROKER, config.MQTT_PORT, 60)
        
        # Boucle d'écoute
        if duration:
            print(f"⏱️  Écoute pendant {duration} secondes...\n")
            client.loop_start()
            time.sleep(duration)
            client.loop_stop()
        else:
            print("⏱️  Écoute continue (Ctrl+C pour arrêter)...\n")
            client.loop_forever()
        
        return handler.devices_data
        
    except Exception as e:
        print(f"✗ Erreur MQTT: {e}")
        return {}


def main():
    """Fonction principale"""
    
    # Récupérer les capteurs depuis config
    sensors = config.ZIGBEE_SENSORS
    
    # Connexion à TimescaleDB
    connection_string = (
        f"host={config.TIMESCALEDB_HOST} "
        f"port={config.TIMESCALEDB_PORT} "
        f"user={config.TIMESCALEDB_USER} "
        f"password={config.TIMESCALEDB_PASSWORD} "
        f"dbname={config.TIMESCALEDB_DATABASE} "
        f"sslmode={config.TIMESCALEDB_SSLMODE}"
    )
    
    conn = None
    try:
        conn = psycopg2.connect(connection_string)
        print("✓ Connecté à TimescaleDB\n")
        
        # Écouter les messages MQTT et envoyer dans TimescaleDB
        # duration=None pour écoute continue (pour tests: duration=60)
        listen_mqtt(sensors, db_conn=conn, duration=None)
        
    except psycopg2.Error as e:
        print(f"✗ Erreur connexion TimescaleDB: {e}")
        print("ℹ️  Écoute sans base de données\n")
        # Écouter quand même sans base de données
        listen_mqtt(sensors, duration=None)
    except KeyboardInterrupt:
        print("\n⚠️  Interruption")
    finally:
        if conn:
            conn.close()
            print("\n✓ Déconnexion TimescaleDB")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Interruption")
        sys.exit(0)

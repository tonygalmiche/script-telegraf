#!/usr/bin/env python3
import psycopg2
from datetime import datetime
import sys
import os
import socket
import config


def get_file_info(file_path, name):
    """Récupère les informations d'un fichier"""
    try:
        if not os.path.exists(file_path):
            print(f"Erreur: Le fichier '{file_path}' n'existe pas")
            return None
        
        # Récupérer les statistiques du fichier
        file_stats = os.stat(file_path)
        
        # Date de dernière modification (timestamp)
        modification_time = datetime.fromtimestamp(file_stats.st_mtime)
        
        # Taille du fichier en octets
        file_size = file_stats.st_size
        
        # Nom du fichier (sans le chemin)
        file_name = os.path.basename(file_path)
        
        # Nom d'hôte
        hostname = socket.gethostname()
        
        return {
            'file_path': file_path,
            'file_name': file_name,
            'name': name,
            'hostname': hostname,
            'modification_time': modification_time,
            'file_size': file_size
        }
    except Exception as e:
        print(f"Erreur lors de la récupération des informations du fichier: {e}")
        return None


def write_to_timescaledb(file_info):
    """Écrit les informations du fichier dans TimescaleDB"""
    try:
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
        cursor = conn.cursor()
        
        timestamp = datetime.now()
        
        # Insertion dans TimescaleDB
        query = """
            INSERT INTO file_info (time, file_path, file_name, name, host, modification_time, file_size)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            timestamp,
            file_info['file_path'],
            file_info['file_name'],
            file_info['name'],
            file_info['hostname'],
            file_info['modification_time'],
            file_info['file_size']
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Formater la date (YYYY-MM-DD HH:MM)
        date_formatted = file_info['modification_time'].strftime('%Y-%m-%d %H:%M')
        
        # Convertir la taille en Mo
        size_mb = file_info['file_size'] / (1024 * 1024)
        
        print(f"Host: {file_info['hostname']} | Name: {file_info['name']} | Fichier: {file_info['file_name']} | Dernière modification: {date_formatted} | Taille: {size_mb:.1f} Mo")
        
        return True
    except Exception as e:
        print(f"Erreur lors de l'écriture dans TimescaleDB: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False


def main():
    """Fonction principale"""
    if len(sys.argv) != 3:
        print("Usage: python3 file-info-to-timescaledb.py <chemin_du_fichier> <name>")
        print("\nExemple:")
        print("  python3 file-info-to-timescaledb.py /var/log/syslog logs_system")
        sys.exit(1)
    
    file_path = sys.argv[1]
    name = sys.argv[2]
    
    # Récupérer les informations du fichier
    file_info = get_file_info(file_path, name)
    
    if file_info is None:
        sys.exit(1)
    
    # Envoyer les données à TimescaleDB
    success = write_to_timescaledb(file_info)
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()

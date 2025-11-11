#!/usr/bin/env python3
import psycopg2
from datetime import datetime, timedelta, timezone
import sys
import config


def get_latest_backups():
    """Récupère les dernières sauvegardes pour chaque host/name"""
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
        
        # Récupérer la dernière entrée pour chaque combinaison host/name
        query = """
            SELECT DISTINCT ON (host, name)
                host,
                name,
                file_path,
                file_name,
                modification_time,
                file_size,
                time
            FROM file_info
            ORDER BY host, name, time DESC
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return results
    except Exception as e:
        print(f"Erreur lors de la récupération des données: {e}")
        return None


def analyze_backups(backups):
    """Analyse les sauvegardes et détecte les problèmes"""
    # Utiliser le fuseau horaire de Paris (UTC+1 ou UTC+2 selon DST)
    # Pour simplifier, on utilise le fuseau horaire local
    now = datetime.now(timezone.utc)
    limit_24h = now - timedelta(hours=24)
    
    alerts = []
    ok_count = 0
    
    print("=" * 120)
    print(f"ANALYSE DES SAUVEGARDES - {now.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 120)
    print()
    
    for backup in backups:
        host, name, file_path, file_name, modification_time, file_size, time = backup
        
        # S'assurer que modification_time est aware (avec fuseau horaire)
        if modification_time.tzinfo is None:
            # Si naive, on considère qu'il est en UTC
            modification_time = modification_time.replace(tzinfo=timezone.utc)
        
        # Calculer l'âge de la sauvegarde
        age = now - modification_time
        age_hours = age.total_seconds() / 3600
        
        # Convertir la taille en Mo
        size_mb = file_size / (1024 * 1024)
        
        has_alert = False
        alert_messages = []
        
        # Vérifier si la modification date de plus de 24h
        if modification_time < limit_24h:
            has_alert = True
            alert_messages.append(f"⚠️  ALERTE: Sauvegarde trop ancienne ({age_hours:.1f}h)")
        
        # Vérifier si la taille est 0
        if file_size == 0:
            has_alert = True
            alert_messages.append("⚠️  ALERTE: Fichier vide (0 octets)")
        
        # Afficher les informations
        status = "❌ PROBLÈME" if has_alert else "✓ OK"
        print(f"{status} | Host: {host:<30} | Name: {name:<15} | Taille: {size_mb:>8.1f} Mo | Âge: {age_hours:>6.1f}h")
        
        if has_alert:
            for msg in alert_messages:
                print(f"     {msg}")
            print(f"     Fichier: {file_path}")
            print()
            alerts.append({
                'host': host,
                'name': name,
                'file_path': file_path,
                'modification_time': modification_time,
                'file_size': file_size,
                'age_hours': age_hours,
                'messages': alert_messages
            })
        else:
            ok_count += 1
    
    print("=" * 120)
    print(f"RÉSUMÉ: {ok_count} sauvegarde(s) OK | {len(alerts)} alerte(s)")
    print("=" * 120)
    
    return alerts


def main():
    """Fonction principale"""
    print("Récupération des sauvegardes depuis TimescaleDB...")
    print()
    
    backups = get_latest_backups()
    
    if backups is None:
        print("Erreur lors de la récupération des sauvegardes")
        sys.exit(1)
    
    if len(backups) == 0:
        print("Aucune sauvegarde trouvée dans la base de données")
        sys.exit(0)
    
    alerts = analyze_backups(backups)
    
    # Code de sortie : 0 si OK, 1 si des alertes
    sys.exit(1 if len(alerts) > 0 else 0)


if __name__ == "__main__":
    main()

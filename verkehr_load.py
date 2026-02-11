import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import os
import time
import rdflib
from bs4 import BeautifulSoup

def load_data(path_data, path_info, url_meta):
    """Lädt Verkehrsdaten aus Datei."""
    #Datensatz abrufen
    display_data = "data/" + path_data +".csv"
    df_data = pd.read_csv(display_data, delimiter=";")

    #Informationsdatensatz abrufen
    display_info = "data/" + path_info +".csv"
    df_display_info = pd.read_csv(display_info, delimiter=",")
    '''
    #Metadatensatz abrufen
    url = "https://offenedaten-konstanz.de/dcatapde.xml"
    response = requests.get(url)
    g = rdflib.Graph()
    g.parse(data=response.content, format="xml")
    DCT = rdflib.namespace.DCTERMS
    uri = rdflib.URIRef(url_meta)
    title = str(g.value(uri, DCT.title)) or "Kein Titel gefunden"
    soup = BeautifulSoup(g.value(uri, DCT.description), "html.parser")
    varnames_dict = {}
    for li in soup.find_all("li"):
        if ":" in li.text:
            key, val = li.text.split(":", 1)
            varnames_dict[key.strip()] = val.strip()
    '''    
    return df_data, df_display_info

def erstelle_tabelle(title_data, title_meta):
    
    """Erstellt die Tabelle falls sie nicht existiert."""
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
    
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {title_data} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            zeit TIMESTAMP,
            richtung INTEGER,
            anzahl_messungen INTEGER,
            anzahl_fahrzeuge INTEGER,
            durchschnittsgeschwindigkeit FLOAT,
            hoechstgeschwindigkeit FLOAT,
            info VARCHAR(128),
            strasse VARCHAR(128),
            hausnummer VARCHAR(8)
        );
    """)
    
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {title_meta} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strasse VARCHAR(128),
            size INTEGER,
            start TIMESTAMP,
            ende TIMESTAMP
            strasse_geodaten VARCHAR(128),
            lat FLOAT,
            lon FLOAT
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Tabellen erstellt.")
    

def speichere_in_postgres(title_data, data, title_meta, meta):
    """Speichert DataFrame in PostgreSQL Datenbank."""

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()

    for _, row in data.iterrows():
        cur.execute(f"""
            INSERT INTO {title_data} (datum, richtung, anzahL_messungen, anzahl_fahrzeuge, durchschnittsgeschwindigkeit, hoechstgeschwindigkeit, info, strasse, hausnummer)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row.get["datum"],
            row.get("richtung"),
            row.get("anzahL_messungen"),
            row.get("anzahl_fahrzeuge"),
            row.get("durchschnittsgeschwindigkeit"),
            row.get("hoechstgeschwindigkeit"),
            row.get("info"),
            row.get("strasse"),
            row.get("hausnummer"),
        ))
        
    for _, row in meta.iterrows():
        cur.execute(f"""
            INSERT INTO {title_meta} (strasse, size, start, ende, strasse_geodaten, lat, lon)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row.get["strasse"],
            row.get("size"),
            row.get("start_datum"),
            row.get("end_datum"),
            row.get("strasse_geodaten"),
            row.get("lat"),
            row.get("lon"),
        ))

    conn.commit()
    cur.close()
    conn.close()

def main():
    
    dict_years = {
        2020: ("Geschwindigkeitsdisplays_2020", "gdinfo_2020", "https://offenedaten-konstanz.de/dataset/geschwindigkeits-berwachung/resource/e3f0c3ed-e08f-47d2-a90e-7e0d5d803910"),
        2021: ("Geschwindigkeitsdisplays_2021", "gdinfo_2021", "https://offenedaten-konstanz.de/dataset/geschwindigkeits-berwachung/resource/6f5f79a9-83c6-4abc-991f-c30e65f81f4a"),
        2022: ("Geschwindigkeitsdisplays_2022", "gdinfo_2022", "https://offenedaten-konstanz.de/dataset/geschwindigkeits-berwachung/resource/88011736-3690-4ed3-af75-43212291c403"),
        2023: ("Geschwindigkeitsdisplays_2023", "gdinfo_2023", "https://offenedaten-konstanz.de/dataset/geschwindigkeits-berwachung/resource/92cb6449-8566-46b1-bfdc-f698eadfd8d2"),
        2024: ("Geschwindigkeitsdisplays_2024", "gdinfo_2024", "https://offenedaten-konstanz.de/dataset/geschwindigkeits-berwachung/resource/76aa67f3-38b0-49c0-9a56-8d0fff300247")
    }
    
    for key, value in dict_years.items():
        data = load_data(value[0], value[1], value[2])
        erstelle_tabelle(value[0], value[1])
        speichere_in_postgres(value[0], data[0], value[1], data[1])
        print(data)
        
if __name__ == "__main__":
    main()
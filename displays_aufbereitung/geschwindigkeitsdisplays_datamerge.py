# -*- coding: utf-8 -*-
"""
Created on Fri May 23 12:37:29 2025

@author: DigIT-DuS6
"""

import pandas as pd
from fuzzywuzzy import fuzz
import osmnx as ox
from shapely.geometry import Point
import re
from datetime import timedelta
import math

#Input Files
display_location = r"./Standorte Geschwindigkeitstafeln.csv"
geodata_location = r"//Netapp2/Digit-DUS6$/Documents/Gebäude_Hausnummer.csv"

#fixes
# Straße, Hausnummer, Hausnummer_Neu
fix_housenumber = r"./house_number_fix.csv"
# Str Straße, Str Straße_Neu
fix_streetname = r"./streetname_fix.csv"
# Tuple (Straße, Hausnummer), Lat, Lon
fix_coordinates = r"./coordinates.csv"

#Zwischenspeichern
#Funktion 1: Häuser mit gültiger Hausnummer
valid_housenumber = r"./valid_housenumber.csv"

#Funktion 2: Validierte Hausnummern
validated_housenumber = r"./validated_houses.csv"

#Funktion 3: Angepasster Datensatz
corrected_houses = r"./corrected_houses.csv"

#generell ungültige Häuser
invalid_houses = r"./invalid_locations.csv"

#Funktion 4: Häuser mit Geodaten aus GIS
geodata_houses = r"./valid_geodata.csv"

#Funktion 4: Häuser ohne Geodaten aus GIS
geodata_invalid = r"./invalid_geodata.csv"

#Funktion 5: Auf Straßen platzierte positionen
geodata_streets = r"./position_street.csv"

#Funktion 6: Display Datensatz
display_data = r"../../data/Geschwindigkeitsdisplays_2024.csv"

#Funktion 6: Output File
df_gdinfo = r"./gdinfo_2024.csv"

"""DATENAUFBEREITUNG"""

#df2 = pd.read_csv(r"//Netapp2/Digit-DUS6$/Documents/Liste Messstellen sloc.csv", delimiter=";")
#df_displays = pd.read_csv(r"//Netapp2/Digit-DUS6$/Documents/Geschwindigkeitsdisplays_2024.csv", delimiter=";")

#Geht über alle Einträge drüber, schreibt alle gültigen und ungültigen Einträge in separate Dataframes
#Datum hat wenige falsche Einträge, diese werden händisch im Skript korrigiert nachdem die Falschen Einträge gefunden wurden
#Haus-Nr. gibt die Straße und den Hausnummer-Eintrag aus und erwartet einen Konsolen-Input (im Idealfall die richtige Hausnummer)
#Am Ende werden die Einträge mit gültigen Hausnummern im Dataframe als csv datei mit dem namen valid_houses exportiert
#Für ungültige Hausnummern wird der korrigierte Wert in einer separaten Spalte "Haus-Nr.(korr)" hinterlegt und zum prüfen/weiterarbeiten in eine
#csv mit dem Namen invalid_houses exportiert
def clean_data():
    
    df_standorte = pd.read_csv(display_location, delimiter=";")
    
    #Datum
    #Leere Dataframes für falsche Einträge erstellen
    invalid_dates = df_standorte[df_standorte["Haus-Nr."]==1]
    #Falsche Datumseintraege finden
    for index, row in df_standorte.iterrows():
        try:
            row['Start'] = pd.to_datetime(row['Start'], dayfirst=True)
            row['Ende'] = pd.to_datetime(row['Ende'], dayfirst=True)
        except Exception:
            print("Fehler gefunden bei Index {}, Start: {}, Ende: {}".format(index, row['Start'], row['Ende']))
            invalid_dates.loc[len(invalid_dates)] = row
    
    for index, row in df_standorte.iterrows():
        if row['Start'] > row['Ende']:
            print("Fehler gefunden bei Index {}, Start: {}, Ende: {}".format(index, row['Start'], row['Ende']))
    
    
    #Falsche Einträge korrigieren soweit möglich
    """
    Ergebnisse
    index 159: ----- -> drop line
    index 280: 15.05.-2023 -> 15.05.2023
    index 330: 09*.10.2023 -> 09.10.2023
    index 475: 14.04.20205 -> 14.04.2025"""
    df_standorte.drop([159], inplace=True)
    df_standorte.loc[280, 'Ende'] = "15.05.2023"
    df_standorte.loc[330, 'Ende'] = "09.10.2023"
    df_standorte.loc[475, 'Ende'] = "14.04.2025"
    df_standorte = df_standorte.dropna(subset=["Start"])
    
    #Hausnummern
    #Leere Dataframes für richtige und falsche Einträge erstellen
    df_standorte["Haus-Nr.(korr)"] = None
    valid = df_standorte[df_standorte["Haus-Nr."]==1]
    invalid = df_standorte[df_standorte["Haus-Nr."]==1]
    
    fix_data = pd.read_csv(fix_housenumber, delimiter=",")
    adresses = []
    for index, row in fix_data.iterrows():
        adresses.append((row["strasse"], row["hausnummer"]))
    house = fix_data["korrigierte_hausnummer"]
    
    #Ungültige Hausnummern finden (alle die nicht als Int geparset werden können oder in das Muster Xy passen wo X eine Zahl und Y ein Buchstabe ist)
    for index, row in df_standorte.iterrows():
        
        #automatische korrektur von einträgen
        strasse = (row["Strasse"], row['Haus-Nr.'])
        if strasse in adresses:
            list_index = adresses.index(strasse)
            row["Haus-Nr."] = house[list_index]
        
        try:
            row['Haus-Nr.'] = int(row['Haus-Nr.'])
        except Exception:
            pattern = r"^\d+[a-z]"
            string = str(row["Haus-Nr."])
            if re.match(pattern, string):
                valid.loc[len(valid)] = row
            else:
                invalid.loc[len(invalid)] = row
        else:
            valid.loc[len(valid)] = row
    
    #Fehlerbehebung
    """
    list_nhnr = []
    #Manuelle Korrektur
    for index, row in invalid_houses.iterrows():
        strasse = row["Strasse"]
        old = row['Haus-Nr.']
        new = input("Die eingetragene Adresse lautet {} {}: ".format(strasse, old))
        
        row['Haus-Nr.'] = new
        list_nhnr.append(new)

    invalid_houses["Haus-Nr.(korr)"] = list_nhnr
    """
    
    valid.to_csv(valid_housenumber, index=False)
    invalid.to_csv(invalid_houses, index=False)


#im nächsten Schritt werden invalid_houses und valid_houses zusammengeführt
#invalid_houses werden hier auf übereinstimmung von alten und korrigierten Wert geprüft (für hausnummern mit Buchstaben etc)
#Die Restlichen Hausnummern werden nochmal Manuell angepasst 
def validate_data():
    valid = pd.read_csv(valid_housenumber, delimiter=",")
    invalid = pd.read_csv(invalid_houses, delimiter=",")
    
    test = invalid[invalid["Haus-Nr."]==1]
    
    for index, row in invalid.iterrows():
        if row['Haus-Nr.'] == row['Haus-Nr.(korr)']:
            valid.loc[len(valid)] = row
        else:
            test.loc[len(test)] = row
 
    
    #prüfen ob adresseinträge konsistent sind
    entries = []
    house = []
    df_index = []
    
    for index, row in test.iterrows():
        entry = (row["Strasse"], str(row["Haus-Nr."]))
        value = row["Haus-Nr.(korr)"]
        if entry in entries:
            list_index = entries.index(entry)
            if house[list_index] == value:
                None
            else:
                print("Addressen von Eintrag {} und Eintrag {} stimmen nicht überein".format(df_index[list_index], index))
        else:
            entries.append(entry)
            house.append(value)
            df_index.append(index)
            
    """
    #mismatch bei 0 und 10
    #mismatch bei 85 und 88
    test.loc[0, 'Haus-Nr.(korr)'] = 18
    test.loc[88, 'Haus-Nr.(korr)'] = 34
    """
            
    """        
    #dataframe für das prüfen zukünftiger Daten
    dictionary = {
        "strasse_hausnummer": entries,
        "korrigierte_hausnummer": house
        }
    
    fix_data = pd.DataFrame(dictionary)
    fix_data.to_csv(r"./house_number_fix_new.csv", index=False)
    """
    
    
    #Nochmal gucken welche Einträge nach validieren der korrigierten Hausnummern übrig bleiben
    leftover_data = test[test["Haus-Nr."]==1]
    
    for index, row in test.iterrows():
        try:
            row["Haus-Nr.(korr)"] = int(row["Haus-Nr.(korr)"])
        except Exception:
            pattern = r"^\d+[a-z]"
            string = str(row["Haus-Nr.(korr)"])
            if re.match(pattern, string):
                valid.loc[len(valid)] = row
            else:
                leftover_data.loc[len(leftover_data)] = row
        else:
            valid.loc[len(valid)] = row
    
    valid.to_csv(validated_housenumber, index=False)
    leftover_data.to_csv(invalid_houses, index=False)     
    
#Daten (Haus-Nummern) eintragen/überschreiben
def correct_data():
    valid = pd.read_csv(validated_housenumber, delimiter=",")
    invalid = pd.read_csv(invalid_houses, delimiter=",")
    
    valid = valid.drop(columns=['GeDi \nNummer', 'ohne Panel', 'Unnamed: 12','Unnamed: 13','Unnamed: 14'])
    invalid = invalid.drop(columns=['GeDi \nNummer', 'ohne Panel', 'Unnamed: 12','Unnamed: 13','Unnamed: 14'])
    
    valid["Zusatz"] = None
    invalid["Zusatz"] = ''
    
    for index, row in valid.iterrows():
        if not(row.isnull().loc["Haus-Nr.(korr)"]):
           row["Haus-Nr."] = row["Haus-Nr.(korr)"]
           
        value = row["Haus-Nr."]
        letters = ""
        numbers = ""
        for char in value:
            if char.isdigit():
                numbers += char
            else:
                letters += char
        
        row["Haus-Nr."] = numbers
        row["Zusatz"] = letters
        
        value = row["Strasse"]
        strassenname_norm = value.replace("Str.", "Straße").replace("Strasse", "Straße").replace("str.", "straße").replace("strasse", "straße").replace("oe", "ö").replace("ae", "ä")
        row["Strasse"] = strassenname_norm
        
        valid.loc[index] = row
    
    valid.to_csv(corrected_houses, index=False)
    invalid.to_csv(invalid_houses, index=False)   

def add_geodata():
    valid = pd.read_csv(corrected_houses, delimiter=",")
    invalid = pd.read_csv(invalid_houses, delimiter=",")
    df_geodaten = pd.read_csv(geodata_location, delimiter=";")
    
    valid = valid.fillna('')
    
    agg_data = df_geodaten.groupby("Strassenname").agg({'Hausnummer.Id': ['min']})
    agg_data = agg_data.reset_index()
    agg_data.columns = ["strasse", "strasse_id"]
    id_liste = agg_data["strasse_id"].tolist()
    id_strassen = [e[0:5] for e in id_liste]
    agg_data["strasse_id"] = id_strassen
    
    #Id der Strassen holen
    streetid = []
    street_corr = []
    for index, row in valid.iterrows():
        strassen = row["Strasse"]
        
        strassen_fix = pd.read_csv(fix_streetname, delimiter=",")
        strassen_fix_strassen = strassen_fix["strasse"].tolist()
        strassen_fix_fixstrassen = strassen_fix["korrigierte_straße"].tolist()
        
        if strassen in strassen_fix_strassen:
            list_index = strassen_fix_strassen.index(strassen)
            strassen = strassen_fix_fixstrassen[list_index]
        
        temp_df = agg_data[agg_data["strasse"] == strassen].reset_index()
        a_temp_list = []
        if temp_df.empty:
            for index_2, row_2 in agg_data.iterrows():
                fuzzy = fuzz.ratio(strassen, row_2["strasse"])
                if fuzzy > 85:
                    a_temp_list.append((fuzzy, row_2["strasse"], row_2["strasse_id"]))
                    
        if a_temp_list:   
            a_best_fit = a_temp_list[0]
            if len(a_temp_list) == 1:
                None
            else:
                for entry in a_temp_list:
                    if entry[0] > a_best_fit[0]:
                        a_best_fit = entry
            streetid.append(a_best_fit[2])
            street_corr.append(a_best_fit[1])
            
        elif not(temp_df.empty):
            streetid.append(temp_df.loc[0, 'strasse_id'])
            street_corr.append(temp_df.loc[0, 'strasse'])
            
        else:
            streetid.append("no match found")
        
    invalid["id_strasse"] = ''
    invalid["strasse_corr"] = ''
    valid["strasse_corr"] = street_corr
    valid["id_strasse"] = streetid
    
    test = valid[valid["id_strasse"]=="no match found"]
    test.to_csv('unmatched_streets.csv', index=False)
    
    #Hausnummern erstellen/normalisieren und ID-Daraus gewinnen
    houseid = []
    for index, row in valid.iterrows():
        value = str(int(row["Haus-Nr."]))
        
        if len(value) < 3:
            while len(value) < 3:
                value = "0"+value
            houseid.append(str(row["id_strasse"]) + str(value) + str(row["Zusatz"]))
        else:
            houseid.append(str(row["id_strasse"]) + str(value) + str(row["Zusatz"]))
            
    invalid["id_standort"] = ''
    valid["id_standort"] = houseid

    lat = []
    lon = []
    
    new_valid = valid[valid["Haus-Nr."]==0]
    new_invalid = invalid[invalid["Haus-Nr."]==0]
    
    for index, row in valid.iterrows():
        
        id = row["id_standort"]
        print(id)
        temp_df = df_geodaten[df_geodaten["Hausnummer.Id"] == id].reset_index()
        if temp_df.empty:
            new_invalid.loc[len(new_invalid)] = row
        else:
            new_valid.loc[len(new_valid)] = row
            lat.append((temp_df.at[0, "Latitude"]).replace(",", "."))
            lon.append((temp_df.at[0, "Longitude"]).replace(",", "."))
            
    
    new_invalid["lat"] = ''
    new_valid["lat"] = lat
    new_invalid["lon"] = ''
    new_valid["lon"] = lon
    
    index = ["start", "ende", "strasse", "geschwindigkeit", "hausnummer", "fahrtrichtung", "reichweite", "akkuwechsel", "anmerkung1", "anmerkung2", "hausnummer_geodaten", "hausnummer_zusatz", "strasse_geodaten", "id_strasse", "id_standort", "lat_geodaten", "lon_geodaten"]
    
    new_valid.columns = index
    new_invalid.columns = index
    
    new_valid.to_csv(geodata_houses, index=False)
    new_invalid.to_csv(geodata_invalid, index=False)

def move_geo_to_street():
    
    valid = pd.read_csv(geodata_houses, delimiter=",")
    complete = valid[valid["strasse"] == 0]
    complete["lat_osm"] = None
    complete["lon_osm"] = None
    
    try:
        df = pd.read_csv(geodata_streets, delimiter=",")
    except:
        None
    else:
        #df = df.drop(columns=['_merge'])
        complete = df
    
    index = ["start", "ende", "strasse", "hausnummer", "fahrtrichtung", "reichweite", "anmerkung1", "anmerkung2", "hausnummer_geodaten", "strasse_geodaten", "id_strasse", "lat_geodaten", "lon_geodaten"]
    
    match = pd.merge(valid, complete, on=index, how='left', indicator=True)
    
    valid = match[match["_merge"]=="left_only"].reset_index()
    
    #find nearest street node to the adress
    for index, row in valid.iterrows():
        point = (row["lat_geodaten"], row["lon_geodaten"])
        
        lazy_dict = {"Zeller Straße": "Zellerstraße"}
        
        if row["strasse_geodaten"] in lazy_dict.keys():
            row["strasse_geodaten"] = lazy_dict[row["strasse_geodaten"]]
        
        distance = 400
        filtered_gdf = []
        print("{} of {}".format(index, len(valid)))
        print(row["strasse_geodaten"])
        while not filtered_gdf:
            print("Trying {}m".format(distance))
            mapdata = ox.graph_from_point(point, dist=distance, network_type='drive')
            
            # Convert the graph to a GeoDataFrame of edges
            edges_gdf = ox.graph_to_gdfs(mapdata)
            
            gdf_nodes = edges_gdf[0]
            gdf_edges = edges_gdf[1]
            
            test = gdf_edges[gdf_edges['name'] == row["strasse_geodaten"]]
            
            
            for index_2, row_2 in gdf_edges.iterrows():
                row['u'] = index_2[0]
                row['v'] = index_2[1]
                row['key'] = index_2[2]
                if isinstance(row_2['name'], str):
                    a_fuzzymatch=fuzz.ratio(row_2['name'], row["strasse_geodaten"])
                    if a_fuzzymatch >= 90:
                        filtered_gdf.append(row_2)
                elif isinstance(row_2['name'], list):
                    filtered_gdf.append(row_2) if row["strasse_geodaten"] in row_2['name'] else None
            distance += 150
            
        
        #gdf_edges = gdf_edges[gdf_edges['name'] in row["display.strasse"]]
        test = gdf_edges.crs
        gdf_edges_2 = pd.DataFrame(filtered_gdf)
        gdf_edges_2.index.names = ['u', 'v', 'key']
        gdf_edges_2.crs = test
        
        if not(gdf_edges.empty):
            mapdata = ox.graph_from_gdfs(gdf_nodes, gdf_edges_2)
            nearest_edge = ox.distance.nearest_edges(mapdata, X=point[1], Y=point[0])
        
        gdf_edges_2 = gdf_edges.reset_index()

        edge = gdf_edges_2[(gdf_edges_2['u'] == nearest_edge[0])& (gdf_edges_2['v'] == nearest_edge[1])]

        # Access the geometry column of the edges    
        edge_geom = edge['geometry']
        edge_geom = edge_geom.reset_index()
        
        # create shapely point geometry object as (x, y), that is (lng, lat)
        point_geom = Point(reversed(point))
        # use shapely to find the point along the edge that is closest to the reference point
        nearest_point_on_edge = edge_geom.interpolate(edge_geom.project(point_geom))
        coords = (nearest_point_on_edge[0].x, nearest_point_on_edge[0].y)
        row['lat_osm'] = coords[1]
        row['lon_osm'] = coords[0]
        
        complete.loc[len(complete)] = row
        complete.to_csv(geodata_streets, index=False)
        
    complete.to_csv(geodata_streets, index=False)
    
    
def match_displaydata():
    
    df_locations = pd.read_csv(geodata_streets)
    df_displays = pd.read_csv(display_data, delimiter=";")
    
    df_locations = df_locations.drop(columns=['geschwindigkeit', 'reichweite', 'akkuwechsel', 'anmerkung1', 'anmerkung2', 'hausnummer_geodaten', 'hausnummer_zusatz', 'strasse', 'id_strasse', 'id_standort', 'lat_geodaten', 'lon_geodaten'])
    
    df_locations['start'] = pd.to_datetime(df_locations['start'], dayfirst=True)
    df_locations['ende'] = pd.to_datetime(df_locations['ende'], dayfirst=True)
    df_displays['datum'] = pd.to_datetime(df_displays['datum'])
    
    size = df_displays.groupby(by = ['strasse']).agg({'datum': ['min', 'max'], 'anzahl_messungen': ['sum']})
    size['days_difference'] = (size[('datum', 'max')] - size[('datum', 'min')]).dt.days
    size['messungen_adj'] = size[("anzahl_messungen", "sum")] / size["days_difference"]
    range_max = size["messungen_adj"].max()
    range_min = size["messungen_adj"].min()
    new_max = 20
    new_min = 2
    
    size["size"] = round(((size["messungen_adj"]-range_min) / (range_max-range_min)) * (new_max-new_min) + new_min)
    size.columns = ("d_min", "d_max", "m", "d_d", "m_adj", "size")
    size = size.drop(columns=["d_min", "d_max", "m", "d_d", "m_adj"])
    size = size.reset_index()
    
    #Filtern der Displays nach einzelnen Standorten
    df_displays_streets = size
    #Droppen der nicht notwendigen Felder
    start_datum = []
    end_datum = []
    for index, row in df_displays_streets.iterrows():
        place = row["strasse"]
        new_df = df_displays[df_displays["strasse"] == place]
        min_date = new_df['datum'].min()
        max_date = new_df['datum'].max()
        start_datum.append(min_date)
        end_datum.append(max_date)
        #Textausgabe wenn Strasse von Geschwindigkeitsdisplays immernoch nicht in den Standorten gefunden wird
        #if row['strasse'] not in strassennamen_norm:
            #print(index)
    #Fuer Zuordnung start und end-datum der Geschwindigkeitsdisplays eintragen
    df_displays_streets["start_datum"] = start_datum
    df_displays_streets["end_datum"] = end_datum
    
    test = pd.DataFrame()
    
    for index, row in df_displays_streets.iterrows():
        
        df_filtered_streets = df_locations[df_locations["strasse_geodaten"] == row["strasse"]]
        
        #Edgecases: Grundschule Wollmatingen, Schwaketenstraße, Mainaustraße Höhe schule für 2024
        dict_edgecases = {"Grundschule Wollmatingen": ("Schwaketenstraße", [3]),
                          "Mainaustraße Hoehe Schule": ("Mainaustraße", [147,148]),
                          "Mainaustraße": ("Mainaustraße", [96]),
                          "Schwaketenstraße": ("Schwaketenstraße", [110])}
        
        if row["strasse"] in dict_edgecases.keys():
            values = dict_edgecases[row["strasse"]]
            
            df_filtered_streets = df_locations[df_locations["strasse_geodaten"] == values[0]]
            df_filtered_streets = df_filtered_streets[df_filtered_streets["hausnummer"].isin(values[1])]
    
        timespan_start_df = df_filtered_streets[(df_filtered_streets["start"] <= row["start_datum"])]
        timespan_start_df = timespan_start_df[timespan_start_df["ende"] > row["start_datum"]]
        
        timnespan_end_df = df_filtered_streets[(df_filtered_streets["ende"] + timedelta(days=1)) >= row["end_datum"]]
        timnespan_end_df = timnespan_end_df[(timnespan_end_df["start"] + timedelta(days=1)) < row["end_datum"]]
        
        indices = ["start", "ende", "strasse_geodaten"]
        match = pd.merge(timespan_start_df, timnespan_end_df, on=indices, how='outer', indicator=True)
        match["merge_index"] = index
        
        test = pd.concat([test, match], ignore_index=True)
        
        
    list_lat = []
    list_lon = []
    for index, row in test.iterrows():
        print(row["lat_osm_x"])
        lat = row["lat_osm_x"] if not (math.isnan(row["lat_osm_x"])) else row["lat_osm_y"]
        lon = row["lon_osm_x"] if not (math.isnan(row["lon_osm_x"])) else row["lon_osm_y"]
        
        list_lat.append(lat)
        list_lon.append(lon)
        
    test['lat'] = list_lat
    test['lon'] = list_lon

    drop_rows = ["hausnummer_x", "hausnummer_y", "fahrtrichtung_x", "fahrtrichtung_y", "_merge", "lon_osm_y", "lon_osm_x", "lat_osm_y", "lat_osm_x"]
    test = test.drop(columns=drop_rows)
    
    df_displays_streets["merge_index"] = df_displays_streets.index
    
    df_display_coordinates = pd.merge(df_displays_streets, test, on="merge_index", how='outer', indicator=True)
    
    df_display_coordinates.to_csv(df_gdinfo)
    
    
    
#clean_data()
#validate_data()
#correct_data()
#add_geodata()
#move_geo_to_street()
match_displaydata()



#Strassennamen normaliesieren und Matches zwischen Standort und Displays finden
liste = df_liste['Strasse'].tolist()
merge_index = []
list_strasse = []
for i in range(len(liste)):
    strassenname = liste[i]
    #Normalisieren nach Geschwindigkeitsdisplay Satz
    strassenname_norm = strassenname.replace("Str.", "Straße").replace("Strasse", "Straße").replace("str.", "straße").replace("strasse", "straße").replace("oe", "ö").replace("ae", "ä")
    list_strasse.append(strassenname_norm)
    #Matches finden mit Fuzzy comparison
    match = -1
    for index, row in df_messung_nd.iterrows():
        a_fuzzymatch=fuzz.ratio(strassenname_norm, row["strasse"])
        if a_fuzzymatch >= 85:
            match = row["merge_index"]
    merge_index.append(match)      
#Fuer Merge gefundene Indizes als Column speichern
df_liste["merge_index"] = merge_index
df_liste["Strasse"] = list_strasse
#Start und End-Datum der Geschwindigkeitsdisplays ermitteln



join = pd.merge(df_messung_nd, df_liste, on="merge_index")
df_gdinfo = join.groupby(by = ['strasse', 'start_datum', 'end_datum']).agg({'Start': ['min', lambda x: list(x)], 'Ende':['max', lambda x: list(x)], 'Strasse': lambda x: list(x), 'Haus-Nr.': lambda x: list(x), 'Geschwindigkeit': lambda x: list(x), 'Fahrtrichtung': lambda x: list(x)})

df_gdinfo = df_gdinfo.reset_index()
columns = ["display.strasse", "display.start_datum", "display.end_datum", "standort.start_min", "standort.start_entries", "standort.ende_max", "standort.ende_entries", "standort.strasse", "standort.hausnummer", "standort.geschwindigkeit", "standort.fahrtrichtung"]
df_gdinfo.columns = columns

#expand display_info for easier access
display_info_expanded = []
for index, row in df_gdinfo.iterrows():
    temp = row["standort.start_entries"]
    for i, name in enumerate(temp):
        temp = [row["display.strasse"],row["display.start_datum"],row["display.end_datum"],row["standort.start_min"],row["standort.start_entries"][i],row["standort.ende_max"],(row["standort.ende_entries"])[i],row["standort.strasse"][i],row["standort.hausnummer"][i],row["standort.geschwindigkeit"][i],row["standort.fahrtrichtung"][i],row["id_strasse"], row["id_standort"][i], row["lat"][i], row["lon"][i]]
        display_info_expanded.append(temp)

df_gdinfo_expanded = pd.DataFrame(display_info_expanded, columns=["display.strasse", "display.start_datum", "display.end_datum", "standort.start_min", "standort.start_entry", "standort.ende_max", "standort.ende_entry", "standort.strasse", "standort.hausnummer", "standort.geschwindigkeit", "standort.fahrtrichtung", "geo.id_strasse", "geo.id_standort", "geo.lat", "geo.lon"])
df_gdinfo_expanded["geo.lat_float"] = pd.to_numeric(df_gdinfo_expanded["geo.lat"])#df_gdinfo_expanded["geo.lat"].astype(float)
df_gdinfo_expanded["geo.lon_float"] = pd.to_numeric(df_gdinfo_expanded["geo.lon"])
    

df_gdinfo_expanded.to_csv(r"//Netapp2/Digit-DUS6$/Documents/gdinfo_expanded.csv", sep=";")
df_gdinfo.to_csv(r"//Netapp2/Digit-DUS6$/Documents/gdinfo.csv", sep=";")




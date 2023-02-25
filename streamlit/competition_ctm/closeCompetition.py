import pandas as pd
from bigquery import Bigquery as bg
from geopy.distance import geodesic

class CloseCompetition :

    def getBusiness (id_business: int) :
        return bg.searchBusiness(id_business)
    
    def getCompetition (latitude: float, longitude: float, distance: int) :

        df_competition = bg.searchCompetition(latitude, longitude, distance)

        #se calcula la distancia exacta de la competencia
        bus_coordinates = (latitude, longitude)
        df_competition['distance'] = df_competition.apply(lambda row: geodesic(bus_coordinates, (row['latitude'], row['longitude']) ).km, axis = 1)

        #se filtra los que estan en el radio pedido y se elimina el negocio propio
        df_competition = df_competition[df_competition['distance'] <= distance ]
        df_competition = df_competition[df_competition['distance'] != 0]

        #se eliminan las columnas innecesarias
        cols = ['latitude', 'longitude']
        df_competition.drop(columns= cols, inplace= True)
        
        return df_competition
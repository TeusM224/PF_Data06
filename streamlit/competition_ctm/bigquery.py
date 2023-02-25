
from google.cloud import bigquery
from bg_key import credentials
import pandas as pd


class Bigquery :
    
    def searchBusiness (id_business: int) :
        query = f'''
        SELECT id_meta, name, avg_rating, num_of_reviews, latitude, longitude,
            FROM `maps_reviews.metadata`
            WHERE id_meta = {id_business}
        '''
        #se crea el cliente
        bqclient = bigquery.Client.from_service_account_json(credentials.path_to_service_account_key_file)

        #se ejecuta la query y se guarda en un dataframe
        df = bqclient.query(query).to_dataframe()

        return df
        
    def searchCompetition (latitude, longitude, distance) :

        #se calcula una latitud min y max para reducir la busqueda inicial
        latitude_min = latitude - (distance * 0.01)#cada 0.0x es aproximadamente x km 
        latitude_max = latitude + (distance * 0.01)

        #se calcula una longitud min y max para reducir la busqueda inicial
        longitude_min = longitude - (distance * 0.01)
        longitude_max = longitude + (distance * 0.01)

        
        #se hace la query para buscar la competencia
        query = f'''
        SELECT id_meta, name, avg_rating, num_of_reviews, latitude, longitude
            FROM `maps_reviews.metadata`
        WHERE
            latitude BETWEEN  {latitude_min} AND {latitude_max}
            AND longitude  BETWEEN {longitude_min} AND {longitude_max}
        '''
        #se crea el cliente
        bqclient = bigquery.Client.from_service_account_json(credentials.path_to_service_account_key_file)

        #se ejecuta la query y se guarda en un dataframe
        df = bqclient.query(query).to_dataframe()

        return df
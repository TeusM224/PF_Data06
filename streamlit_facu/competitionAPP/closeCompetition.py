import pandas as pd
from geopy.distance import geodesic

#modulos creados por mi
from competitionAPP.queries import Queries

class CloseCompetition :

    def get_business (id_business: int) :
        return Queries.search_Business(id_business)
    
    def get_competition (latitude: float, longitude: float, distance: int) :

        df_competition = Queries.search_Competition(latitude, longitude, distance)

        #se calcula la distancia exacta de la competencia
        bus_coordinates = (latitude, longitude)
        df_competition['distance'] = df_competition.apply(lambda row: geodesic(bus_coordinates, (row['latitude'], row['longitude']) ).km, axis = 1)

        #se filtra los que estan en el radio pedido y se elimina el negocio propio
        df_competition = df_competition[df_competition['distance'] <= distance ]
        df_competition = df_competition[df_competition['distance'] != 0]

        
        return df_competition
    
    def get_listBusiness (isHotel: bool) :
        listBus = Queries.search_ListBusiness(isHotel)

        #se unen el nombre y el id en una sola columna
        listBus['name-id'] = listBus['name'] + '-' + listBus['id_meta'].astype(str)

        #se crea una tupla con la nueva columna
        listBus = tuple(listBus['name-id'])

        return listBus
    

    def get_listFomParquet(tipe: str) :
        listBus = pd.read_parquet(f'list{tipe}.parquet')
        listBus = tuple(listBus['name-id'])
        
        return listBus

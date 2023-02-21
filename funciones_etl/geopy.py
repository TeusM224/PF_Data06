from geopy.geocoders import Nominatim
from time import sleep
from geopy.extra.rate_limiter import RateLimiter
from random import uniform, randint
import pandas as pd
from geopy.exc import GeocoderTimedOut

path_load = '/content/drive/MyDrive/geopy/meta9.csv'#aca la ruta donde esta el archivo a leer
location_df = pd.read_csv(path_load, usecols=['gmap_id','address','latitude','longitude'], sep=';')

location_df['coordenadas'] = location_df['latitude'].astype(str) + ' , ' + location_df['longitude'].astype(str)


location_df = location_df[['gmap_id','address','coordenadas']]
location_df.rename(columns={'address':'direccion'}, inplace = True)



#Funcion Geopy con Nominatium


#geolocator = Nominatim(user_agent="probandogeo")
#geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)


geolocator = Nominatim(user_agent="meta 11")
#reverse = RateLimiter(geolocator.reverse, min_delay_seconds=0.0005)
reverse = RateLimiter(geolocator.reverse)

def getting_state(coord):

  

  try:
    sleep(uniform(0.0005,0.0015))
    location = reverse(coord, exactly_one=True, language='en')
    address = location.raw['address']

    state_country = address.get('state', '')+'|'+address.get('county', '')
    return state_country

  except GeocoderTimedOut:
    print('time out in coor: '+str(coord))
    sleep(randint(11, 20))
    location = reverse(coord, exactly_one=True, language='en')
    address = location.raw['address']

    state_country = address.get('state', '')+'|'+address.get('county', '')
    return state_country
  except KeyError:
    return '<Na>|<Na>'


n_rows = len(location_df.index)
print(n_rows)
i = 0 #en caso de error inicializar desde donde se quiere volver a empezar
f = i+1000
b = True
while b :
  if f > n_rows :
    f = n_rows
    b= False
  sub_df = location_df.iloc[i:f].copy()

  sub_df['state_country'] = sub_df['coordenadas'].apply(lambda coord: getting_state(coord))
  sub_df['state'] = sub_df['state_country'].apply(lambda s: s.split('|')[0])
  sub_df['country'] = sub_df['state_country'].apply(lambda s: s.split('|')[1])
  sub_df.drop(['state_country'], axis=1, inplace=True)

  
  path_save = '/content/drive/MyDrive/geopy/meta9.csv'#aca la ruta donde se guarda el nuevo archivo
  sub_df.to_csv('/content/drive/MyDrive/geopy/geopy9.csv', sep=';', index = False, mode='a', header= False)
  print('finish i: '+str(i)+'  f: '+str(f))
  i += 1000
  f += 1000
  
  sleep(10)


from google.cloud import bigquery
import pandas as pd

#modulos creados por mi
#from competitionAPP.bg_key import credentials

#path = "competitionAPP/bg_key/southern-ivy-378521-58bc704ea77d.json"
#bqclient = bigquery.Client.from_service_account_json(path)

credentials = {
  "type": "service_account",
  "project_id": "southern-ivy-378521",
  "private_key_id": "58bc704ea77d9096da196d378ad5111ffe49ac12",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC4AAGabqs6rg3O\nbymhyNgSjZ4n5ep06IKY3rDvR3wLHAQOzTWk+dlxADjrc40Q7wS+X76l7ov2QjW/\n/UQaXZza/ZCFWdRNlT4wdpgyqpN/Ds4WHvTAvyTCEZg7U9peR5YtWAbRDN50hM6p\nQQvkl9u+s8TLaGH6ELR0UwXP7Jv+8Le0yA3vf8ZG40xQQgj74pnlEag1uju1sgZ+\nEs31WqRhvk30RXTdHLLmolx9OojH2W9T0FHvxWdJPIHjRMfdM2YHWZjowGsR77UD\nmS720N4i36x0aSgzrOYiqnLl1elx3TqBd6ImWFT3t3C6TwWMTsAl2m6oI1K23hda\n7lD1pwWhAgMBAAECggEAVvKhz1FGlscS/R7ohGv9Nt83AlGSNkZ9GDH7WEbmZfWK\nMVhlZh0u1EgvnYuP+JWKH0/tLkoIV4k4Dw5mNTNRYOZ7eZhWS4cfyRTxnNegNWVQ\nm4FrVP2+J/rafgaE5RJc8/Zp2jWDlXoAkliLfy0HiRk4AIrF6b4lF+b3kyaTHKfk\nNKdAFF0PxpzRgR2yxjo+L4wzT5CU9dsdaTFvAM82xCUi8Nesu54trHWBaQsZmEKr\njBXdyeMThfeNmlI9fzCvrgS8JIWakQgplaf+1VI2pNazkkCLPs6qX9cO8/iErlt4\nVVA2ZqsCIV0Bc7v57+KXTWpJHKY/a+HlMoJrW+NwZQKBgQDkZaK34ZhtfpGUVN+X\nOTsw2Gk4BBwJxt1zXq6w7G6O2NdnTomB/eFSqr3T3EnyyxB2eHYpcn6tv07HSS3Y\nw9nymTyuelV5EsaZj461QCoRc9Np4myVm79Ge6XeoUg+/Wk5Mi8JaK5Gt+H6efyP\nxjv0/LjeHV723hUfUVUZFoaXtwKBgQDOPMY3unQ0NOi6H508RQFtYfrWlSEBYZMe\no6MZ6puXELGbcuIEyGBS8ddoaQWo5dgCmLr7lJaj58rMiFnxPqVFUJDO8ZLOCG3r\nWl6BxUH97yRk80fGjX8xNcYanKkJ3ayMo/e2hGZn2nI3xsdZQ6jPNgvpJj7XJXX9\npaDwqbHdZwKBgEgAEk/NenPqFpKgPuw5SoOXdXQHN5+NZXpdOszje+bkTONwSsED\n++hHkxgateUoRsRSLU3bnju/t2Hzm9RdyXNFA3AteIx2cs0uCFrzI0/lJ5yVmI2B\nT8c0a5HHXjMdR/KT82gP7AC3LzH7Crd4UqZklRLQR4Ojdx0sUptaEov5AoGBAKIU\njy8i4Mpavn/v8pF4JdwCAbF8d/ju50FQFxF6GJdLblmdmtVpaJInMEQFLYaERBQe\nVez75LuhofbGPfFja12jKHsKzXYQl9o8JkPUc72OoMqZi1IWx9wzx6IIdqyjQ0Uw\nY7BUM2rw+YyuMuMLUnKupgOwo/hPn4AdF/OrWXaHAoGADz2hkbZ5xS5xOr/Z5PjN\nEi9B4XCMyS/3FzQaLR2gnvNnmtGt73egNZJ10VPO2x9qlSqfkmuX/tI9Q+F3Wwii\nfFoSke2h/k02iLvXEAOx423Sg15EQ1/1MFuXHayUHDMZHCzT2AFSFoGSLOdhw8St\njfh+xJ5KfZrGAlVzLZQBICE=\n-----END PRIVATE KEY-----\n",
  "client_email": "bigquery-user@southern-ivy-378521.iam.gserviceaccount.com",
  "client_id": "105843860503145082352",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bigquery-user%40southern-ivy-378521.iam.gserviceaccount.com"
}


class Queries :
    
    def search_Business (id_business: int) :
        query = f'''
        SELECT id_meta, name, avg_rating, num_of_reviews, latitude, longitude,
            FROM `maps_reviews.metadata`
            WHERE id_meta = {id_business}
        '''
        #se crea el cliente
        bqclient = bigquery.Client.from_service_account_json(credentials)

        #se ejecuta la query y se guarda en un dataframe
        df = bqclient.query(query).to_dataframe()

        return df
        
    def search_Competition (latitude, longitude, distance) :

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
        bqclient = bigquery.Client.from_service_account_json(credentials)

        #se ejecuta la query y se guarda en un dataframe
        df = bqclient.query(query).to_dataframe()

        return df
    
    def search_ListBusiness (isHotel: bool ):

        if isHotel:
            is_not = ''
        else :
            is_not = 'not'
        
        #se hace la query para buscar los negocios dependiendo de si se pidio hotel o los otros
        query = f'''
        select name, id_meta from `maps_reviews.metadata`
        where {is_not} is_hotel
        '''     
        #se crea el cliente
        bqclient = bigquery.Client.from_service_account_json(credentials)
        
        #se ejecuta la query y se guarda en un dataframe
        df = bqclient.query(query).to_dataframe()

        return df

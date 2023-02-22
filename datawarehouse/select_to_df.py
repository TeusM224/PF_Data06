
# modulos a instalar:
#pip install google-cloud
#pip install google-cloud-bigquery
#pip install pandas-gbq


from google.cloud import bigquery
import pandas as pd

bqclient = bigquery.Client.from_service_account_json('aca ponen el path donde esta la key')


sql = """
    SELECT *
    FROM `maps_reviews.reviews`
    WHERE gmap_id = "0x89e4838a126ce667:0x87aa15dbaa467c5a"
    LIMIT 10
"""

result = bqclient.query(sql)

df = result.to_dataframe()

print(df)

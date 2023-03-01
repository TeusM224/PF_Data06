import os
import airflow
from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import pandas as pd
import numpy as np
import regex
# import json
# import pathlib

# import requests
# import requests.exceptions as request_exceptions

# TODO
# Defining google credentials to use
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/osanchezd/Downloads/henry-378006-29bf40f1ae3e.json"

"""
Consider the size of your data
Knowing the size of the data you are passing between Airflow tasks is important when deciding which implementation
method to use. As you'll learn, XComs are one method of passing data between tasks, but they are only appropriate for
small amounts of data. Large data sets require a method making use of intermediate storage and
possibly utilizing an external processing framework.
"""

dag = DAG(
    dag_id="etl_process",
    start_date=airflow.utils.dates.days_ago(2),
    schedule_interval=None,
)


def _load_json():
    df_data = pd.read_json(f"/home/osanchezd/Downloads/PF_Henry/Meta/1.json", lines=True)
    # df_data = pd.read_json('/home/osanchezd/Downloads/PF_Henry/Meta/1.json', lines=True)
    # df_data = pd.read_json('/home/osanchezd/Downloads/PF_Henry/df_full_meta.json', lines=True)
    # df_data = df_data.to_csv(f"/home/osanchezd/Downloads/df_data.csv")
    print(df_data.head())
    print(f'Job load_json done!.')

#
# def _filter_category():
#     df = pd.read_csv("/home/osanchezd/airflow/files/df_data.csv")
#     df['category'] = df['category'].apply(lambda s: str(s).lower() if isinstance(s, list) else 'NO DATA')
#     new_df = pd.DataFrame(columns=df.columns)
#
#     filter = df[df['category'].str.contains('hotel')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('motel')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('hostel')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('breakfast')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('b&b')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('bar')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('diner')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('BBQ')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('Pizza')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('Burger')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('Sandwich')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('Resort')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('Inn')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('suit')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('Heritage')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('dining')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('grill')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('dinner')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('cafeteria')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('restaurant')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('barbecue')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('tavern')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('delicatessen')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('food')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('banquet')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('coffee')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('buffet')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('room')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('lodging')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('pension')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('palace')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('cabin')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('suite')]
#     new_df = pd.concat([new_df, filter])
#     filter = df[df['category'].str.contains('chamber')]
#     new_df = pd.concat([new_df, filter])
#
#     new_df.drop_duplicates(subset='gmap_id', inplace=True)
#
#     new_df['ID_meta'] = np.arange(new_df.shape[0])
#
#     new_df.to_csv("/home/osanchezd/airflow/files/df_data_1.csv")
#     print(f'job category done!')
#
#
# def _drop_columns():
#     df_columns_drops = pd.read_csv("/home/osanchezd/airflow/files/df_data_1.csv")
#     columns_drop = ['address', 'price', 'state', 'relative_results', 'url']
#     df_columns_drops = df_columns_drops.drop(columns=columns_drop, axis=1)
#     df_columns_drops.to_json("/home/osanchezd/airflow/files/df_columns_dropped.json")
#     df_columns_drops.to_csv("/home/osanchezd/airflow/files/df_columns_dropped.csv")
#     print(f'job drops done!.')
#
#
# def _time_day(biglist: list, day: str):
#
#     for smallList in biglist:
#         if smallList[0] == day:
#             return smallList[1]
#
#     return biglist  # por si acaso no encontró el día, que retorne lo mismo que recibió
#
#
# def _table_hours():
#     df_data = pd.read_csv("/home/osanchezd/airflow/files/df_columns_dropped.csv")
#     df_hours = df_data[['gmap_id', 'hours']]
#
#     df_hours['Monday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Monday') if isinstance(hour, list) else hour)
#     df_hours['Tuesday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Tuesday') if isinstance(hour, list) else hour)
#     df_hours['Wednesday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Wednesday') if isinstance(hour, list) else hour)
#     df_hours['Thursday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Thursday') if isinstance(hour, list) else hour)
#     df_hours['Friday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Friday') if isinstance(hour, list) else hour)
#     df_hours['Saturday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Saturday') if isinstance(hour, list) else hour)
#     df_hours['Sunday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Sunday') if isinstance(hour, list) else hour)
#
#     df_hours.drop(['hours'], axis=1, inplace=True)
#     df_hours.to_csv(f"/home/osanchezd/airflow/files/clean_table_hours.csv",
#                     sep=";",
#                     mode='a',
#                     index=False,
#                     encoding='utf-8')
#     df_data.to_csv(f"/home/osanchezd/airflow/files/clean_data.csv",
#                    sep=";",
#                    mode='a',
#                    index=False,
#                    encoding='utf-8')
#     print(f'table_hours job done!.')
#
#
# def _get_misc():
#     # df_data = pd.read_json("/home/osanchezd/airflow/dags/df_columns_dropped.json", lines=True)
#     df_data = pd.read_csv("/home/osanchezd/airflow/files/df_columns_dropped.csv")
#     df = df_data
#     dfMISC = df[['gmap_id', 'MISC']]
#     dfMISC = dfMISC.dropna()
#     dfMISC = dfMISC.join(pd.json_normalize(dfMISC['MISC'])).drop('MISC', axis='columns').reset_index()
#     # borrar = ['index', 'Recycling', 'Getting here', 'Activities']
#     # dfMISC.drop(columns=borrar, inplace=True)
#
#     dfMISC = dfMISC.fillna(
#         {
#             'Service options': 'not_available',
#             'Health & safety': 'not_available',
#             'Accessibility': 'not_available',
#             'Planning': 'not_available',
#             'Offerings': 'not_available',
#             'Amenities': 'not_available',
#             'Atmosphere': 'not_available',
#             'Payments': 'not_available',
#             'Popular for': 'not_available',
#             'Dining options': 'not_available',
#             'Crowd': 'not_available',
#             'From the business': 'not_available',
#             'Highlights': 'not_available'
#         }
#     )
#
#
#     dfMISC[
#         [
#             'Service options', 'Health & safety', 'Accessibility', 'Planning', 'Offerings', 'Amenities', 'Atmosphere',
#             'Payments', 'Popular for', 'Dining options', 'Crowd', 'From the business', 'Highlights'
#         ]
#     ] = dfMISC[
#         [
#             'Service options', 'Health & safety', 'Accessibility', 'Planning', 'Offerings', 'Amenities', 'Atmosphere',
#             'Payments', 'Popular for', 'Dining options', 'Crowd', 'From the business', 'Highlights'
#         ]
#     ].astype(str)
#
#     dfMISC = dfMISC.replace("[", '', regex=True)
#     dfMISC = dfMISC.replace("]", '', regex=True)
#
#     dfMISC.to_csv(f"/home/osanchezd/airflow/files/df_misc.csv",
#                   sep=';',
#                   # mode='a',
#                   index=False,
#                   # header=head_misc,
#                   encoding='utf-8')
#
#     print(f'MISC job done!.')


load_json = PythonOperator(
    task_id='load_json',
    python_callable=_load_json,
    dag=dag,
)
#
# filter_category = PythonOperator(
#     task_id='filter_category',
#     python_callable=_filter_category,
#     dag=dag,
# )
#
# drop_columns = PythonOperator(
#     task_id='drop_columns',
#     python_callable=_drop_columns,
#     dag=dag,
# )
#
# table_misc = PythonOperator(
#     task_id='table_misc',
#     python_callable=_get_misc,
#     dag=dag,
# )
#
# table_hours = PythonOperator(
#     task_id='table_hours',
#     python_callable=_table_hours,
#     dag=dag,
# )
#
# # Uploading transformed data to Cloud Service
# upload_file = LocalFilesystemToGCSOperator(
#     task_id="upload_file",
#     src=f"/home/osanchezd/airflow/files/clean_data.csv",
#     # dst="test_airflow_1/df_misc_dropped.csv",
#     dst="test_airflow_1/",
#     bucket='test_airflow_1',
#     dag=dag,
# )

# load_json >> filter_category >> drop_columns >> table_hours >> table_misc
# load_json >> filter_category >> drop_columns >> table_hours >> table_misc >> upload_file
load_json

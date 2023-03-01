import os
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import pandas as pd
import numpy as np

# Defining google credentials to use
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/osanchezd/Downloads/henry-pf-env-379203-091740f354d0.json"

"""
Consider the size of your data
Knowing the size of the data you are passing between Airflow tasks is important when deciding which implementation
method to use. As you'll learn, XComs are one method of passing data between tasks, but they are only appropriate for
small amounts of data. Large data sets require a method making use of intermediate storage and
possibly utilizing an external processing framework.
"""
df_data = pd.DataFrame()

dag = DAG(
    dag_id="etl_process_full_files",
    start_date=airflow.utils.dates.days_ago(2),
    schedule_interval=None,
)


def _load_json():
    print(f'Job load_json done!.')


def _filter_category():
    df = pd.read_json(f"/home/osanchezd/Downloads/df_full.json")

    df['category'] = df['category'].apply(lambda s: str(s).lower() if isinstance(s, list) else 'NO DATA')
    new_df = pd.DataFrame(columns=df.columns)

    filter = df[df['category'].str.contains('hotel')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('motel')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('hostel')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('breakfast')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('b&b')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('bar')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('diner')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('BBQ')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('Pizza')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('Burger')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('Sandwich')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('Resort')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('Inn')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('suit')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('Heritage')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('dining')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('grill')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('dinner')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('cafeteria')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('restaurant')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('barbecue')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('tavern')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('delicatessen')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('food')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('banquet')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('coffee')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('buffet')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('room')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('lodging')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('pension')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('palace')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('cabin')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('suite')]
    new_df = pd.concat([new_df, filter])
    filter = df[df['category'].str.contains('chamber')]
    new_df = pd.concat([new_df, filter])

    new_df.drop_duplicates(subset='gmap_id', inplace=True)

    new_df['ID_meta'] = np.arange(new_df.shape[0])

    df = new_df.to_csv(f'/home/osanchezd/Downloads/PF_Henry/Clean_data/category.csv')
    print(f'job category done!')


def _drop_columns():
    df_columns_drops = pd.read_csv(f'/home/osanchezd/Downloads/PF_Henry/Clean_data/category.csv')
    columns_drop = ['address', 'price', 'state', 'relative_results', 'url']
    df_columns_drops = df_columns_drops.drop(columns=columns_drop, axis=1)
    df_columns = df_columns_drops.to_csv(f"/home/osanchezd/Downloads/PF_Henry/Clean_data/columns.csv")
    print(f'job drops done!.')


def _time_day(biglist: list, day: str):

    for smallList in biglist:
        if smallList[0] == day:
            return smallList[1]

    return biglist  # Just in case you didn't find the day, return the same thing you received


def _table_hours():
    df_data = pd.read_csv(f"/home/osanchezd/Downloads/PF_Henry/Clean_data/columns.csv")
    df_hours = df_data[['gmap_id', 'hours']]

    df_hours['Monday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Monday') if isinstance(hour, list) else hour)
    df_hours['Tuesday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Tuesday') if isinstance(hour, list) else hour)
    df_hours['Wednesday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Wednesday') if isinstance(hour, list) else hour)
    df_hours['Thursday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Thursday') if isinstance(hour, list) else hour)
    df_hours['Friday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Friday') if isinstance(hour, list) else hour)
    df_hours['Saturday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Saturday') if isinstance(hour, list) else hour)
    df_hours['Sunday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Sunday') if isinstance(hour, list) else hour)

    df_hours.drop(['hours'], axis=1, inplace=True)
    df_hours.to_csv(f"/home/osanchezd/Downloads/PF_Henry/Clean_data/clean_table_hours.csv",
                    sep=";",
                    index=False,
                    )

    print(f'table_hours job done!.')


def _get_misc():
    df = pd.read_csv(f"/home/osanchezd/Downloads/PF_Henry/Clean_data/columns.csv")
    dfMISC = df[['gmap_id', 'MISC']]
    dfMISC = dfMISC.dropna()
    dfMISC = dfMISC.join(pd.json_normalize(dfMISC['MISC'])).drop('MISC', axis='columns').reset_index()
    borrar = ['index', 'Recycling', 'Getting here', 'Activities']
    dfMISC.drop(columns=borrar, inplace=True, errors='ignore')

    dfMISC = dfMISC.fillna('<Na>')
    dfMISC = dfMISC.astype(str)

    columnas = ['gmap_id', 'Service options', 'Accessibility', 'Offerings', 'Amenities', 'Atmosphere',
                'Health & safety', 'Popular for', 'Dining options', 'Crowd', 'Payments', 'Highlights', 'Planning',
                'From the business', 'Health and safety']
    for c in columnas:
        dfMISC[c] = dfMISC.get(c, '<Na>')

    dfMISC = dfMISC.loc[:, columnas]

    dfMISC = dfMISC.replace('\n', '', regex=True)
    dfMISC = dfMISC.replace('\r', '', regex=True)
    dfMISC = dfMISC.replace(';', '..', regex=True)
    dfMISC = dfMISC.replace(r'\[', '', regex=True)
    dfMISC = dfMISC.replace(r'\]', '', regex=True)

    dfMISC.to_csv(f"/home/osanchezd/Downloads/PF_Henry/Clean_data/df_misc.csv", sep=";", index=False)
    print(f'MISC job done!.')


load_json = PythonOperator(
    task_id='load_json',
    python_callable=_load_json,
    dag=dag,
)

filter_category = PythonOperator(
    task_id='filter_category',
    python_callable=_filter_category,
    dag=dag,
)

drop_columns = PythonOperator(
    task_id='drop_columns',
    python_callable=_drop_columns,
    dag=dag,
)

table_misc = PythonOperator(
    task_id='table_misc',
    python_callable=_get_misc,
    dag=dag,
)

table_hours = PythonOperator(
    task_id='table_hours',
    python_callable=_table_hours,
    dag=dag,
)

# Uploading transformed data to Cloud Service
upload_file = LocalFilesystemToGCSOperator(
    task_id="upload_file",
    # TODO src=f"/home/osanchezd/Downloads/PF_Henry/Clean_Data_PF/*.csv",
    # TODO LA BUENA SIMPLE  src=f"/home/osanchezd/Downloads/PF_Henry/Clean_data/df_misc.csv",
    src=f"/home/osanchezd/Downloads/PF_Henry/Clean_data/*.csv",
    # src=f"/home/osanchezd/Downloads/PF_Henry/Clean_data/columns.csv",
    # TODO dst="test_airflow_1/df_misc_dropped.csv",
    dst="henry_clean_data/",
    bucket='henry_clean_data',
    dag=dag,
)

load_json >> filter_category >> drop_columns >> table_hours >> table_misc >> upload_file

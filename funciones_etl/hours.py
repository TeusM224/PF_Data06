import pandas as pd
import numpy as np

def timeDay (bigList: list, day: str) :
    
    for smallList in bigList :
            if smallList[0] == day :
                return smallList[1]
            
    return bigList #por si acaso no encontro el dia, que retorne lo mismo que recibio


def tableHours (df) :

    df['ID_hours'] = np.arange(df.shape[0])

    df_hours = df[['ID_meta', 'hours']]
    
    df_hours['ID_hours'] = np.arange(df_hours.shape[0])
    

    df_hours['Monday'] = df_hours['hours'].apply(lambda hour : timeDay(hour, 'Monday') if isinstance(hour, list) else hour)
    df_hours['Tuesday'] = df_hours['hours'].apply(lambda hour : timeDay(hour, 'Tuesday') if isinstance(hour, list) else hour)
    df_hours['Wednesday'] = df_hours['hours'].apply(lambda hour : timeDay(hour, 'Wednesday') if isinstance(hour, list) else hour)
    df_hours['Thursday'] = df_hours['hours'].apply(lambda hour : timeDay(hour, 'Thursday') if isinstance(hour, list) else hour)
    df_hours['Friday'] = df_hours['hours'].apply(lambda hour : timeDay(hour, 'Friday') if isinstance(hour, list) else hour)
    df_hours['Saturday'] = df_hours['hours'].apply(lambda hour : timeDay(hour, 'Saturday') if isinstance(hour, list) else hour)
    df_hours['Sunday'] = df_hours['hours'].apply(lambda hour : timeDay(hour, 'Sunday') if isinstance(hour, list) else hour)
    
    df_hours.drop(['hours'], axis=1, inplace=True)
    df_hours.to_csv('Data/modelo/hours.csv', sep=';', index = False, mode='a')

    df.drop(['hours'], axis=1, inplace=True)

    return df
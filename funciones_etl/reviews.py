import os
import pandas as pd
import warnings
warnings.filterwarnings(action='ignore')

def statesETL (df_review, id_list) :
    #data = [['tom', 10], ['nick', 15], ['juli', 14], ['jake', 34]]
    #df_review = pd.DataFrame(data, columns=['Name', 'Age'])

    #se eliminan las columnas innecesarias
    delete = ['name', 'pics', 'resp']
    df_review.drop(columns=delete, inplace=True)
    
    #se filtran las reviews de los locales de interes
    df_review = df_review[df_review['gmap_id'].isin(id_list)].copy()

    #se eliminan dulicados
    df_review.drop_duplicates(inplace=True)

    return df_review
  
  
  
path_load = "Data/reviews-estados"
folders = os.listdir(path_load)#se consigue la lista de carpetas de estados

#se obtiene la lista de los ids de los negocios de interes
id_list = pd.read_csv('Data/transformed_data/meta/meta_data.csv', sep=';', usecols=['gmap_id'])
id_list = id_list['gmap_id']#se convierte a serie

for folder in folders :#se recorren las carpetas
    path_files = f"Data/reviews-estados/{folder}"
    files = os.listdir(path_files)#se consigue la lista de archivos de la carpeta

    for file in files :#se recorren los archivos
        path = f'Data/reviews-estados/{folder}/{file}'
        df = pd.read_json(path, lines=True)
        
        df = statesETL(df, id_list)
        df['state'] = folder.replace('review-', '')#se agrega una columna con el estado

        #se guarda
        file = file.replace('.json', '.csv')
        path_save = f'Data/transformed_data/reviews/{folder}/{file}'
        df.to_csv(path_save, sep=';', index=False, header=True, mode='w', escapechar='\\')

        print(f'/{folder}/{file} DONE')

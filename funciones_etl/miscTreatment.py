def misc_table (un_df):

    dfMISC = un_df[['gmap_id','MISC']]
    dfMISC = dfMISC.dropna()
    dfMISC = dfMISC.join(pd.json_normalize(dfMISC['MISC'])).drop('MISC', axis='columns').reset_index()
    borrar = ['index','Recycling','Getting here','Activities']
    dfMISC.drop(columns=borrar, inplace=True, errors='ignore')

    dfMISC = dfMISC.fillna('<Na>')
    dfMISC = dfMISC.astype(str)

    columnas = ['gmap_id', 'Service options', 'Accessibility', 'Offerings', 'Amenities', 'Atmosphere', 'Health & safety', 'Popular for', 'Dining options', 'Crowd', 'Payments', 'Highlights', 'Planning', 'From the business', 'Health and safety']
    for c in columnas :
        dfMISC[c] = dfMISC.get(c, '<Na>')
        
    dfMISC = dfMISC.loc[:, columnas]
    
    dfMISC = dfMISC.replace('\n', '', regex=True)
    dfMISC = dfMISC.replace('\r', '', regex=True)
    dfMISC = dfMISC.replace(';', '..', regex=True)
    dfMISC = dfMISC.replace('\[', '', regex=True)
    dfMISC = dfMISC.replace('\]', '', regex=True)
    
    return dfMISC.to_csv('MISC.csv', sep=';', index = False)

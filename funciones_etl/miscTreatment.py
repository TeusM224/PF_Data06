def misc_table (un_df):

    dfMISC = un_df[['gmap_id','MISC']]
    dfMISC = dfMISC.dropna()
    dfMISC = dfMISC.join(pd.json_normalize(dfMISC['MISC'])).drop('MISC', axis='columns').reset_index()
    borrar = ['index','Recycling','Getting here','Activities']
    dfMISC.drop(columns=borrar, inplace=True) 

    dfMISC = dfMISC.fillna({'Service options':'not_available', 
                            'Health & safety':'not_available', 'Accessibility':'not_available',
                            'Planning':'not_available', 
                            'Offerings':'not_available', 
                            'Amenities':'not_available', 
                            'Atmosphere':'not_available', 
                            'Payments':'not_available', 
                            'Popular for':'not_available', 
                            'Dining options':'not_available', 
                            'Crowd':'not_available',
                            'From the business':'not_available',
                            'Highlights':'not_available'})

    dfMISC[['Service options','Health & safety','Accessibility','Planning', 'Offerings', 'Amenities', 'Atmosphere', 'Payments', 'Popular for', 'Dining options', 'Crowd','From the business','Highlights']] = dfMISC[['Service options','Health & safety','Accessibility','Planning', 'Offerings', 'Amenities', 'Atmosphere', 'Payments', 'Popular for', 'Dining options', 'Crowd','From the business','Highlights']].astype(str)
   
    dfMISC = dfMISC.replace('[ \[ ]', '', regex=True)
    dfMISC = dfMISC.replace('[ \] ]', '', regex=True)
    
    return dfMISC.to_csv('MISC.csv', sep=';', index = False)

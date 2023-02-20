import pandas as pd
import numpy as np

def filterCategory (df) :

    df['category'] = df['category'].apply(lambda s : str(s).lower() if isinstance(s, list) else 'NO DATA')
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

    return new_df
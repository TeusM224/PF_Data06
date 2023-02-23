def convert_date(un_df):

  datess = un_df.time.values.tolist()
  datess_sec = [x/1000 for x in datess]
  newdatess = [datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') for x in datess_sec]
  un_df['new_time'] = pd.DataFrame(newdatess)
  un_df.drop(columns = 'time', inplace = True)
  
  return un_df

#ejemplo dateset de rewiews -> convert_date(df_alabama)

#ejemplo dataset entero de rewiews ->  convert_date(df_location)

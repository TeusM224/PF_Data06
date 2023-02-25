import streamlit as st
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

from closeCompetition import CloseCompetition

#para eliminar los bordes
css = r'''
    <style>
        [data-testid="stForm"] {border: 0px}
    </style>
'''
st.markdown(css, unsafe_allow_html=True)

header = st.container()
input_form = st.form(key='inputs')

prueba = st.container()

with header :
    st.title(':blue[Competition Close to Me]')

with input_form :
    id_hotel = input_form.number_input(label='enter business id', step=1, min_value= 1)
    area = input_form.number_input(label='enter the size of the area (from 1 to 5), for example 3 for an area of 3 km in radius', min_value=1.0, max_value=5.0, step=0.1)
    submit = input_form.form_submit_button('submit')


if submit :
    #se obtiene el df del negocio
    df_business = CloseCompetition.getBusiness(id_hotel)
    
    #se extrae la latitud y longitud, y se eliminan del df
    latitude = df_business.iloc[0]['latitude']
    longitude = df_business.iloc[0]['longitude']
    df_business.drop(columns= ['latitude', 'longitude'], inplace= True)

    #se obtiene el df de los demas negocios dentro del area
    df_competition = CloseCompetition.getCompetition(latitude, longitude, area)


if 'df_business' in locals() and 'df_competition' in locals():
    info_business = st.container()
    info_competition = st.container()
    info_rating = st.container()
    info_reviews = st.container()

    with info_business :
        st.subheader(':orange[Business info]')

        bus_name = df_business.iloc[0]['name']
        bus_rating = df_business.iloc[0]['avg_rating']
        bus_reviews = df_business.iloc[0]['num_of_reviews']
        
        if bus_rating >= 4 :
            bus_colorRat = 'green'
        elif bus_rating >= 3 :
            bus_colorRat = 'orange'
        else :
            bus_colorRat = 'red'


        st.write(f':violet[Name:] {bus_name}')
        st.write(f':violet[avg rating:] :{bus_colorRat}[{bus_rating}]')
        st.write(f':violet[num of reviews:] {bus_reviews}')


    with info_competition :
    
        st.subheader(':orange[Competition]')

        comp_amount = len(df_competition.index)
        comp_rating = df_competition['avg_rating'].mean()
        comp_reviews = df_competition['num_of_reviews'].sum()
        comp_density = comp_amount/area
        
        if comp_density <= 3 :
            comp_colorDens = 'green'
        elif comp_density <= 5 :
            comp_colorDens = 'orange'
        else :
            comp_colorDens = 'red'

        comp_density = str(round(comp_density, 2))
        comp_rating= str(round(comp_rating, 2))


        st.write(f':violet[Competition amount:] {comp_amount}')
        st.write(f':violet[Average rating:] {comp_rating}')
        st.write(f':violet[Total reviews:] {comp_reviews}')
        st.write(f':violet[Competition density per sq. km:] :{comp_colorDens}[{comp_density}]')

    with info_rating :

        fig, ax = plt.subplots()
        hbars = ax.barh(df_competition['name'], df_competition['avg_rating'], align='center', color = '#66C0C0')
        #ax.set_xlabel('avg rating', color= 'w')
        ax.set_title('Average Reviews', color='w')
        ax.axvline(x = 4, color = 'tab:orange', ls='--')

        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        #ax.tick_params(bottom=False, left=False, labelbottom=False)
        ax.set_facecolor("#0E1117")
        fig.set_facecolor('#0E1117')
        #ax.spines['left'].set_color('w')
        #ax.spines['bottom'].set_color('w')
        ax.tick_params(colors='w', which='both', labelbottom=False, bottom=False)
        ax.bar_label(hbars, fmt='%.1f', color= 'w')
        #plt.figure(facecolor='yellow')

        st.pyplot(fig)

    
    with info_reviews :
        fig, ax = plt.subplots()
        hbars = ax.barh(df_competition['name'], df_competition['num_of_reviews'], align='center', color = '#ACCEC0')
        #ax.set_xlabel('avg rating', color= 'w')
        ax.set_title('Num of Reviews', color='w')

        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        #ax.tick_params(bottom=False, left=False, labelbottom=False)
        ax.set_facecolor("#0E1117")
        fig.set_facecolor('#0E1117')
        #ax.spines['left'].set_color('w')
        #ax.spines['bottom'].set_color('w')
        ax.tick_params(colors='w', which='both', labelbottom=False, bottom=False)
        ax.bar_label(hbars, color= 'w')
        #plt.figure(facecolor='yellow')

        st.pyplot(fig)


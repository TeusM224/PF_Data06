import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#modulos creados por mi
from competitionAPP.closeCompetition import CloseCompetition

#se carga la lista de negocios
@st.cache_data
def get_listBusiness (tipe: str) :
    if tipe == 'Hotels' :
        isHotel = True
    else :
        isHotel = False
    listBus = CloseCompetition.get_listBusiness(isHotel)
    #listBus = CloseCompetition.get_listFomParquet(tipe)
    return listBus

@st.cache_data
def get_businesses (id_business: int) :
    #se obtiene el df del negocio
    df_business = CloseCompetition.get_business(id_business)
    
    #se extrae la latitud y longitud, y se eliminan del df
    latitude = df_business.iloc[0]['latitude']
    longitude = df_business.iloc[0]['longitude']

    #se obtiene el df de los demas negocios dentro del area
    df_competition = CloseCompetition.get_competition(latitude, longitude, 3)
    return df_business, df_competition

listBusiness = get_listBusiness('Hotels')


#se crean los contenedores (son como secciones)
header = st.container()
input_business = st.container()
info_business = st.container()
info_competition = st.container()
chart_rating = st.container()
chart_reviews = st.container()
map_competition = st.container()


with header :
    st.title(':blue[Competition Close to...]')

with input_business :

    #se obtiene la lista de negocios y se crea el menu desplegable
    listBusiness = get_listBusiness('Hotels')
    selectBusiness = st.selectbox('search business (name-id)', listBusiness)

    #se separa el id del nombre y se realiza la busqueda de acuerdo con el id
    #para obtener el df del negocio elegido y otro de los negocios alrededor
    id_business = int(selectBusiness.split('-')[-1])
    df_business, df_competition = get_businesses(id_business)


with info_business :
    st.subheader(':orange[Business info]')

    #se extraen los datos del dataframe en variables
    bus_name = df_business.iloc[0]['name']
    bus_rating = df_business.iloc[0]['avg_rating']
    bus_reviews = df_business.iloc[0]['num_of_reviews']
    
    #se establece el color del rating (verde si es bueno, amarillo medio y rojo malo)
    if bus_rating >= 4 :
        bus_colorRat = 'green'
    elif bus_rating >= 3 :
        bus_colorRat = 'orange'
    else :
        bus_colorRat = 'red'

    #se muestran los datos
    st.write(f':violet[Name:] {bus_name}')
    st.write(f':violet[avg rating:] :{bus_colorRat}[{bus_rating}]')
    st.write(f':violet[num of reviews:] {bus_reviews}')


with info_competition :
    st.subheader(':orange[Competition]')

    #se crea el desliasador para seleccionar el tamaño del area
    area = st.slider('enter the size of the area (from 1 to 3), for example 3 for an area of 3 km in radius', min_value=0.1, max_value=3.0, value = 1.0)

    #se filtra el df de los negocios circundantes, de acuerdo al tamaño del area elegida
    df_filter = df_competition[df_competition['distance'] <= area]

    #se calculan los datos con el dataframe y se guardan en variables
    comp_amount = len(df_filter.index)
    comp_rating = df_filter['avg_rating'].mean()
    comp_reviews = df_filter['num_of_reviews'].sum()
    comp_density = comp_amount/area
    
    #se establece el color de la dencidad (verde si es baja, amarillo medio y rojo alta)
    if comp_density <= 3 :
        comp_colorDens = 'green'
    elif comp_density <= 5 :
        comp_colorDens = 'orange'
    else :
        comp_colorDens = 'red'
    #se redondea la dencidad a dos decimales
    comp_density = str(round(comp_density, 2))
    comp_rating= str(round(comp_rating, 2))

    #se muestra la informacion
    st.write(f':violet[Competition amount:] {comp_amount}')
    st.write(f':violet[Average rating:] {comp_rating}')
    st.write(f':violet[Total reviews:] {comp_reviews}')
    st.write(f':violet[Competition density per sq. km:] :{comp_colorDens}[{comp_density}]')

    if len(df_filter.index) >28 :
        st.write('*Some businesses may be omitted from the next two charts because there are too many in the selected area')


#se crea la grafica con el rating de la competencia
with chart_rating :
    
    fig, ax = plt.subplots()
    hbars = ax.barh(df_filter['name'].head(28), df_filter['avg_rating'].head(28), align='center', color = '#66C0C0')

    ax.set_title('Average Reviews', color='w')
    ax.axvline(x = 4, color = 'tab:orange', ls='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.set_facecolor("#0E1117")
    fig.set_facecolor('#0E1117')
    ax.tick_params(colors='w', which='both', labelbottom=False, bottom=False)
    ax.bar_label(hbars, fmt='%.1f', color= 'w')

    st.pyplot(fig)

 
#se crea la grafica con la cantidad de reviews de la cometencia
with chart_reviews :
    fig, ax = plt.subplots()
    hbars = ax.barh(df_filter['name'].head(28), df_filter['num_of_reviews'].head(28), align='center', color = '#ACCEC0')

    ax.set_title('Num of Reviews', color='w')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.set_facecolor("#0E1117")
    fig.set_facecolor('#0E1117')
    ax.tick_params(colors='w', which='both', labelbottom=False, bottom=False)
    ax.bar_label(hbars, color= 'w')

    st.pyplot(fig)


#se muestra un mapa con la ubicacion de la competencia
with map_competition :
    st.map(df_filter)


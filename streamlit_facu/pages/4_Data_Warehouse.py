import streamlit as st
from PIL import Image

st.set_page_config(
    page_title="ETL data warehouse",
    #page_icon="👋",
)

st.markdown("""
# ETL

## 1. Data transformations
In order to prepare the data according to the needs established in the project, the following transformations were performed.

### 1.1 Data from 'Metadata-sitios'
#### - Filter by category
Filter businesses in the hotel and gastronomic sector
""")
       
#st.image('src/datawarehouse/filtro.png', width= 300)
st.image('https://github.com/TeusM224/PF_Data06/blob/main/Assets/src_datawarehouse/filtro.png?raw=true', width= 300)

st.markdown("""
#### - New tables generated
new tables were generated from the misc and hours columns and with the location
""")
            
#st.image('src/datawarehouse/newTables.png', width= 300)
st.image('https://github.com/TeusM224/PF_Data06/blob/main/Assets/src_datawarehouse/newTables.png?raw=true', width= 300)

st.markdown("""
### 1.2 Data from 'Reviews-estados'

#### - Filter rows
Having already filtered the metadata table with the hospitality and gastronomy businesses, the corresponding reviews were filtered from this.

#### - Time transformation
Since the time column was encoded in Unix time format, it was transformed and saved in a date format.
""")

#st.image('src/datawarehouse/time.png')
st.image('https://github.com/TeusM224/PF_Data06/blob/main/Assets/src_datawarehouse/time.png?raw=true')

st.markdown("""
#### - State column
New 'state' column was added to the table.

#### - Dropped columns
Considering that they do not provide relevant information for the objectives of the project, it was decided to eliminate the columns 'name', 'pics', 'resp'

## 2. Enhanced Entity-Relationship (EER) diagram
In this EER diagram you can see the final tables that were obtained after the transformation, including their relationships and columns, as well as the data type of the latter.
""")

#st.image('src/datawarehouse/eer.png')
st.image('https://github.com/TeusM224/PF_Data06/blob/main/Assets/src_datawarehouse/eer.png?raw=true')

st.markdown("""
## 3. Data Loading in Data warehouse
The BigQuery service provided by Google Cloud was chosen to set up the data warehouse. Bigquery is compatible with the SQL language, and is fast for queries in big data sets
""")

#st.image('src/datawarehouse/bigquery.png', width=400)
st.image('https://github.com/TeusM224/PF_Data06/blob/main/Assets/src_datawarehouse/bigquery.png?raw=true', width= 400)

#st.image('src/datawarehouse/bg_screenshot.png', width=450)
st.image('https://github.com/TeusM224/PF_Data06/blob/main/Assets/src_datawarehouse/bg_screenshot.png?raw=true', width= 450)


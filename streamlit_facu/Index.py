# Libreria Streamlit 
import streamlit as st


st.title("Google Maps Recomendations and Reviews ")
st.markdown("***")

tab1, tab2, tab3 = st.tabs(["Google Maps", "Entity-Relation", "Analysis Dashboards"])

with tab1:
    st.header("Google Maps")
    st.image("https://github.com/TeusM224/PF_Data06/blob/main/Assets/google_maps.jpg?raw=true")
    st.write("""Google Maps is a web mapping platform and consumer application offered by Google. It offers satellite imagery, aerial photography, street maps, 360° interactive panoramic views of streets (Street View), real-time traffic conditions, and route planning for traveling by foot, car, bike, air (in beta) and public transportation. As of 2020, Google Maps was being used by over 1 billion people every month around the world.""")
    


with tab2:
    st.subheader("Entity Relationship")
    st.write("""Here we can appreciate the model's entity relation between tables""")
    st.image("https://github.com/TeusM224/PF_Data06/blob/main/Assets/eer.png?raw=true")

with tab3:
    st.header("Analysis")
    st.write('The following images were extracted from the PowerBi presentation. You can find the whole PBIX files in the following Google Drive Link:')
    st.write('https://drive.google.com/drive/folders/1WP7KSAlK6TuET9_crPDCdd9QWw1XXKKv?usp=share_link')
    st.image("https://github.com/TeusM224/PF_Data06/blob/main/Assets/dash_1.jpeg?raw=true")
    st.write()
    st.image("https://github.com/TeusM224/PF_Data06/blob/main/Assets/dash_2.jpeg?raw=true")
    st.write()
    st.image("https://github.com/TeusM224/PF_Data06/blob/main/Assets/dash_3.jpeg?raw=true")     
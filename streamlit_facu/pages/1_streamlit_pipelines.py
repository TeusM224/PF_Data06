import streamlit as st
from PIL import Image

st.set_page_config(
    page_title="Airflow Pipeline",
    page_icon="👋",
)

st.write("# Pipeline (Airflow)")

st.sidebar.success("Overview")

st.markdown(
    """
    # **Tasks**    
"""
)
# tasks
st.image("https://github.com/TeusM224/PF_Data06/blob/main/Assets/src_airflow/1.jpg")
st.markdown(
    """
    ## **Running tasks**    
"""
)
# running tasks
st.image("https://github.com/TeusM224/PF_Data06/blob/main/Assets/src_airflow/2.jpg")

st.markdown(
    """
    ## **Filter category task job done!**    
"""
)
# filter category task success job done!
st.image("https://github.com/TeusM224/PF_Data06/blob/main/Assets/src_airflow/5.jpg")

st.markdown(
    """
    ## **Upload successful**    
"""
)
# upload successful
st.image("https://github.com/TeusM224/PF_Data06/blob/main/Assets/src_airflow/3.jpg")

st.markdown(
    """
   ## **Bucket data**    
"""
)
# Bucket data
st.image("https://github.com/TeusM224/PF_Data06/blob/main/Assets/src_airflow/4.jpg")



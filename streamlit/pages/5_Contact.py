import streamlit as st

st.header("Contact us!")
# Facundo
c_1,c_2,c_3 = st.columns([3,4,1])
with c_1:
    st.subheader('Facundo Gabriel Arce')
with c_2:
    st.write('')
    if st.checkbox("Contact Facundo"):
            
            st.markdown("facuarce96@gmail.com")
            st.subheader("Linkedin Facundo")
            st.markdown("https://www.linkedin.com/in/facundo-gabriel-arce-21aa49165/")
with c_3:
    st.markdown("")

# Mateo
cm_1,cm_2,cm_3 = st.columns([3,4,1])
with cm_1:
    st.subheader('Mateo Murillo')
with cm_2:
    st.write('')
    if st.checkbox("Contact Mateo"):
            st.markdown("teusmurillo224@gmail.com")
            st.subheader("Linkedin Mateo")
            st.markdown("https://www.linkedin.com/in/mateomurillo224/")
with cm_3:
    st.markdown("")
# Juan Camilo
cj_1,cj_2,cj_3 = st.columns([3,4,1])
with cj_1:
    st.subheader('Juan Camilo')
with cj_2:
    st.write('')
    if st.checkbox("Contact Juan"):
            st.markdown("jcamilo9219@gmail.com")
            st.subheader("Linkedin Juan Camilo")
            st.markdown("-")
with cj_3:
    st.markdown("")

# Diego
cd_1,cd_2,cd_3 = st.columns([3,4,1])
with cd_1:
    st.subheader('Diego Morales')
with cd_2:
    st.write('')
    if st.checkbox("Contact Diego"):
            st.markdown("diegomoralesostos.opsu@gmail.com")
            st.subheader("Linkedin Diego")
            st.markdown("https://www.linkedin.com/in/diegomoos/")
with cd_3:
    st.markdown("")
# Oscar
co_1,co_2,co_3 = st.columns([3,4,1])
with co_1:
    st.subheader('Oscar Sanchez')
with co_2:
    st.write('')
    if st.checkbox("Contact Oscar"):
            st.markdown("oscar.sz.dz@gmail.com")
            st.subheader("Linkedin Oscar")
            st.markdown("https://www.linkedin.com/in/oscar-s%C3%A1nchez-d%C3%ADaz-72262590/")
with co_3:
    st.markdown("")

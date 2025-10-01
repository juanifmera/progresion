import streamlit as st
from datetime import datetime
import pandas as pd
import os
from utils.utils import exporto_parquet, proteger_pagina

st.set_page_config(layout='wide')
proteger_pagina()

def format_size(bytes_size: int) -> str:
    """Convierte bytes a formato legible (KB, MB o GB)."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024 #type:ignore
    return f"{bytes_size:.2f} TB"

st.title('Convertir archivos **MUY PESADOS** a Parquet')
st.divider()
st.markdown('Utiliza esta herramienta para poder convertir archivos PESADOS de excel o CSV en Parquet para optimizar las demas herramientas del sistema')

st.subheader('Primeros Pasos', divider='rainbow')

with st.container(border=True):
    st.markdown('**1- PRIMER PASO**: Carga porfavor el archivo que quieras convertir en formato Parquet', help='Esta funcionalidad convierte archivos xlsx y csv a parquet, reduciendo su peso y optimizando las lecturas del mismo en las demas herramientas')

    archivo_a_convertir = st.file_uploader('Colocar aqui archivo CSV o XLSX', type=['csv', 'xlsx'], accept_multiple_files=False)

    if archivo_a_convertir:
        st.success(f'Archivo Cargado Correctamente')
    else:
        st.warning('No tienes nada cargado')

with st.container(border=True):
    st.markdown('**2- SEGUNDO PASO**: Una vez cargado el archivo lo vas a poder pre-visualizar una muestra random de 20 registros. Los archivos de Micro nativamente tienen las columnas en la segunda/tercer fila. Es posible que si hayas cambiado la estructura original del archivo, no veas los datos de forma correcta')

    if archivo_a_convertir and archivo_a_convertir is not None:
        nombre = archivo_a_convertir.name #type:ignore
        extension = os.path.splitext(nombre)[1]

        if extension == '.csv':
            archivo_a_convertir = pd.read_csv(archivo_a_convertir, encoding='utf-16', header=1, sep=',') #type:ignore

            st.dataframe(archivo_a_convertir.sample(20))

        elif extension == '.xlsx':
            archivo_a_convertir = pd.read_excel(archivo_a_convertir, header=1)

            st.dataframe(archivo_a_convertir.sample(20))
    else:
        st.info('Falta cargar archivo para poder cargar los datos')

with st.container(border=True):
    st.markdown('**3- TERCER PASO**: Descargar el Archivo en Formato Parquet!')

    if archivo_a_convertir is not None:
        parquet_file = exporto_parquet(archivo_a_convertir) #type:ignore
        descargar_parquet = st.download_button('Descargar Archivo en formato Parquet',file_name=f'{os.path.splitext(nombre)[0]}.parquet', data=parquet_file, mime="application/octet-stream", use_container_width=True, type='primary')

        if descargar_parquet:
            original_size = archivo_a_convertir.size  # en bytes
            parquet_size = len(parquet_file.getbuffer())  #type:ignore

            st.success('Archivo convertido existosamente')

    else:
        st.write('Una vez que hayas subido el archivo podras convertirlo a Parquet')
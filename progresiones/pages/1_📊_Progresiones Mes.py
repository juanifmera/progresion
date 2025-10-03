import streamlit as st
from datetime import datetime
from utils.utils import progresiones_mmaa, proteger_pagina
import pandas as pd

st.set_page_config(layout='wide')
proteger_pagina()

st.title('Calcular Progresiones Mes Actual contra mismo Mes del año pasado')
st.divider()
st.markdown('Utiliza esta herramienta para poder obtener las progresiones a mes cerrado a nivel Compañia, Formato, Provincia, Tienda, Sector, Seccion y Grupo de Familia')

st.subheader('Primeros Pasos', divider='rainbow')

col1, col2, col3 = st.columns(3)

with col1:
    with st.container(border=True):
        st.markdown('**1- PRIMER PASO**: Carga porfavor el archivo de **Ventas y Volumen** en formato CSV de MicroStrategy', help='Listar informacion de Micro de Ventas y Volumen utilizando los siguiente filtros: Año, Mes, Direccion, Punto Operacional, Sector, Seccion, Grupo de Familia, Estructura Comercial = Listar Sectores, Empresa = Todos los formatos, incluidos E-commerce y No Informado, Periodo = El mes a analizar 2025 y 2024.')

        ventas_y_volumen = st.file_uploader('Colocar aqui archivo CSV de Ventas y Volumen', type=['csv'], accept_multiple_files=False)

        if ventas_y_volumen:
            st.success(f'Archivo Ventas y Volumen cargado Correctamente')
        else:
            st.info('Falta Cargar Archivo de VCT y VOL')

with col2:
    with st.container(border=True):
        st.markdown('**2- SEGUNDO PASO**: Carga porfavor el archivo de **Debitos** en formato CSV de MicroStrategy', help='Listar informacion de Micro de Debitos utilizando los siguiente filtros: Año, Mes, Direccion, Punto Operacional, Sector, Seccion, Grupo de Familia, Estructura Comercial = Listar Sectores, Empresa = Todos los formatos, incluidos E-commerce y No Informado, Periodo = El mes a analizar 2025 y 2024.')

        debitos = st.file_uploader('Colocar aqui archivo CSV de Debitos', type=['csv', 'parquet'], accept_multiple_files=False)

        if debitos:
            st.success(f'Archivo Debitos cargado Correctamente')
        else:
            st.info('Falta Cargar Archivo de Debitos')

with col3:
    with st.container(border=True):
        st.markdown('**3- TERCER PASO**: Carga porfavor la ultima version del **Padron** en formato XLSX desde Drive', help='Listar informacion de Micro de Debitos utilizando los siguiente filtros: Año, Mes, Direccion, Punto Operacional, Empresa = Todos los formatos, incluidos E-commerce y No Informado, Periodo = El mes a analizar 2025 y 2024.')

        padron = st.file_uploader('Colocar aqui el Padron en formato Excel', type=['xlsx','parquet'], accept_multiple_files=False)

        if padron:
            st.success(f'Archivo Padron cargado Correctamente')
        else:
            st.info('Falta Cargar Padron')

st.subheader('Ya casi Estamos ...', divider='rainbow')
st.markdown('Una vez que esten subidos los tres archivo, debajo aparecerá una lista de meses para seleccionar. Este campo sera utilizado para realizar la comparabilidad de superficies entre tiendas.')


if ventas_y_volumen and debitos and padron:
    # datetime.today().month devuelve 1-12, por eso restamos 1 para el índice
    meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

    mes_actual = meses[datetime.today().month - 2]

    mes = st.selectbox('Elegir un mes para realizar la comparabilidad de calculos para las progresiones.', meses, index=datetime.today().month - 2, placeholder=mes_actual)

    calculate = st.button('¡¡¡Calcular Progresiones MMAA!!!', type='primary', use_container_width=True)

    if calculate:
        with st.spinner("🔄 Calculando progresiones y generando archivo Excel (Tiempo Estimado 1 min)"):
            excel_file = progresiones_mmaa(ventas_y_volumen, debitos, padron, mes)

            if excel_file is not None:
                st.download_button("📥 Descargar Excel", data=excel_file, file_name=f"Progresiones MMAA - {mes}.xlsx", use_container_width=True,  mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.write(excel_file)
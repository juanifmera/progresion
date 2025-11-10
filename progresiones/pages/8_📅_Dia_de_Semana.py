import streamlit as st
from datetime import datetime
from utils.utils import proteger_pagina, dia_de_semana
import pandas as pd
import io

st.set_page_config(layout='wide')
proteger_pagina()

st.title('📅 Obtención del dia de semana')
st.divider()
st.markdown('Utilizá esta herramienta para obtener el detalle de dia de semana de cualquier reporte. Tene en cuenta que el flujo devuelve no solamente el detalle del dia de la semana, sino tambien una columna del mes y año.')

# ---------- SUBIR ARCHIVOS ----------
st.subheader('Paso 1 - Carga de Archivos', divider='rainbow')

col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.markdown('**1. Archivo que se desea verificar el dia de semana**')
        archivo = st.file_uploader("📁 Subí el archivo Deseado", type=["csv"])

        if archivo:
            st.success("Archivo cargado correctamente")
        else:
            st.info("Falta subir este archivo")

with col2:

    with st.container(border=True):
        st.markdown('**3. Padrón (XLSX)**')
        padron = st.file_uploader("📁 Subí el Padrón actualizado", type=["xlsx"])

        if padron:
            st.success("Archivo cargado correctamente")
        else:
            st.info("Falta subir este archivo")

# ---------- SELECCION DE MES ----------
st.subheader('Paso 2 - Elegir el Mes de Comparabilidad', divider='rainbow')

meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
mes_actual = meses[datetime.today().month - 2]
mes_comparable = st.selectbox("🗓️ Elegí el mes a comparar", meses, index=meses.index(mes_actual))

# ---------- BOTON CALCULAR ----------
st.subheader('Paso 3 - Generar Reportes', divider='rainbow')

if archivo and padron:
    st.markdown("✅ Todos los archivos fueron cargados correctamente. Ya podés transformar el archivo subido para obtener el detalle de los dias de semana!.")

    if st.button("🚀 Generar Transformacion", type="primary", use_container_width=True):
        with st.spinner("⏳ Generando transformaciones ... Tiempo estimado: 30 Segundos"):
            
            output = dia_de_semana(archivo_csv=archivo, mes_comparable=mes_comparable, padron=padron)
            if isinstance(output, bytes) or isinstance(output, io.BytesIO):
                st.success("✅ Transformaciones realizadas correctamente.")
                st.download_button(
                    label="📦 Descargar Reporte",
                    data=output,
                    file_name=f"{archivo.name} - {mes_comparable}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            else:
                st.error(f'Ocurrio un error. Error: {output}')
else:
    st.warning("⚠️ Faltan archivos por cargar. Subí todos los archivos para poder continuar.")


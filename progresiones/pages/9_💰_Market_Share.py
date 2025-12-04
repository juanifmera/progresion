import streamlit as st
import pandas as pd
from utils.utils import (
    proteger_pagina,
    marketshare,
    padron_marketshare,
    carga_padron,
    carga_share,
)
from datetime import datetime

# Config inicial
st.set_page_config(layout="wide")
proteger_pagina()

# Inicializar session_state
if "datos_transformados" not in st.session_state:
    st.session_state["datos_transformados"] = False

# Título
st.title("📊 Actualización de Datos - Market Share")
st.divider()
st.markdown(
    "Esta herramienta permite transformar y subir mensualmente los datos de Market Share a GCP. "
    "Asegurate de cargar los archivos correctamente antes de confirmar la subida."
)

# ----------- PASO 1: CARGA DE ARCHIVOS -----------
st.subheader("Paso 1 - Cargar Archivos", divider="rainbow")

with st.expander("¡¡¡Revisar información importante!!!"):
    st.code(
        '''
1. Market Share
    - Revisar que todas las columnas sean datetime
    - Quitar columnas extra en blanco
    - La automatización únicamente subirá los datos del mes "vencido". 
      Esto quiere decir que si se corre en Noviembre, con la info de Octubre, 
      solo se subirá Octubre al histórico.

2. Padrón
    - Confirmar que esté actualizado. No se necesita hacer limpieza previa.

3. Reiniciar
    - En caso de querer reiniciar el proceso de cero, aseguresé de apretar el boton debajo para limpiar los 'session state' de streamlit.
'''
    )
    # Botón opcional para reiniciar todo
    if st.button("🔁 Reiniciar Proceso", width='stretch', type='secondary'):
        st.session_state.clear()
        st.rerun()

col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.markdown("📁 **Archivo de Market Share (Excel)**")
        marketshare_data = st.file_uploader("Subí el archivo mensual", type=["xlsx"])
        if marketshare_data:
            st.success("✅ Archivo de Market Share cargado correctamente.")
        else:
            st.info("⏳ Aún falta cargar este archivo.")

with col2:
    with st.container(border=True):
        st.markdown("📁 **Archivo del Padrón (Excel)**")
        padron_data = st.file_uploader("Subí el padrón actualizado", type=["xlsx"])
        if padron_data:
            st.success("✅ Archivo del padrón cargado correctamente.")
        else:
            st.info("⏳ Aún falta cargar este archivo.")

# ----------- PASO 2: TRANSFORMAR DATOS -----------
st.subheader("Paso 2 - Previsualizar Datos Transformados", divider="rainbow")

if marketshare_data and padron_data:
    st.markdown(
        "✅ Archivos cargados correctamente. Podés transformar los datos para verificar que todo esté bien antes de subir a GCP."
    )

    if st.button("🔄 Transformar Datos", width='stretch', type="primary"):
        with st.spinner("Transformando archivos..."):
            resultado_marketshare = marketshare(marketshare_data)
            resultado_padron = padron_marketshare(padron_data)

        if isinstance(resultado_marketshare, pd.DataFrame) and isinstance(resultado_padron, pd.DataFrame):
            st.session_state["resultado_marketshare"] = resultado_marketshare
            st.session_state["resultado_padron"] = resultado_padron
            st.session_state["datos_transformados"] = True
            st.success("✅ Datos transformados exitosamente. Podes ver una muestra random de 20 registros.")
        else:
            st.session_state["datos_transformados"] = False
            st.error(
                f"❌ Hubo un error al transformar los datos:\n\n"
                f"- Marketshare: {resultado_marketshare}\n"
                f"- Padrón: {resultado_padron}"
            )
else:
    st.warning("⚠️ Faltan archivos. Subí ambos archivos para continuar.")

# ----------- PASO 3: SUBIDA A GCP -----------
if st.session_state["datos_transformados"]:
    resultado_marketshare = st.session_state["resultado_marketshare"]
    resultado_padron = st.session_state["resultado_padron"]

    with st.expander("🧾 Previsualizar - Datos de Market Share"):
        st.dataframe(resultado_marketshare.sample(20), width='stretch')

    with st.expander("🏪 Previsualizar - Padrón Actualizado"):
        st.dataframe(resultado_padron.sample(20), width='stretch')

    st.subheader("Paso 3 - Subida Final a GCP", divider="rainbow")
    st.markdown("Si los datos previsualizados son correctos, podés subirlos a BigQuery.")

    if st.button("🚀 Subir a BigQuery", width='stretch', type='primary'):
        with st.spinner("Subiendo a GCP..."):
            resultado_padron_gcp = carga_padron(padron=resultado_padron)
            resultado_share_gcp = carga_share(share_data=resultado_marketshare)

        if resultado_padron_gcp.startswith('Error'):
            st.error(f"📌 Padrón: {resultado_padron_gcp}")
        else:
            st.success(f"📌 Padrón: {resultado_padron_gcp}")

        if resultado_share_gcp.startswith('Error'):
            st.error(f"📊 Market Share: {resultado_share_gcp}")
        else:
            st.success(f"📊 Market Share: {resultado_share_gcp}")
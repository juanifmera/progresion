import streamlit as st
import pandas as pd
from utils.utils import proteger_pagina, marketshare, carga_padron, carga_share  # Vos te encargás de modularizar esto
from datetime import datetime

st.set_page_config(layout="wide")
proteger_pagina()

st.title("📊 Actualización de Datos - Market Share")
st.divider()
st.markdown("Esta herramienta permite transformar y subir mensualmente los datos de Market Share a GCP. Asegurate de cargar los archivos correctamente antes de confirmar la subida.")

# ----------- PASO 1: CARGA DE ARCHIVOS -----------
st.subheader("Paso 1 - Cargar Archivos", divider="rainbow")

with st.expander('¡¡¡Revisar informacion importante!!!'):
    st.code('''
    1. Market Share
        - Revisar que todas las columnas sean datetime
        - Quitar columnas extra en blanco

    2. Padrón
        - Confirmar que esté actualizado
    ''')

col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.markdown("📁 **Archivo de Market Share (Excel)**")
        marketshare_data = st.file_uploader("Subí el archivo mensual", type=["xlsx"], help='Para mas ifnormacion comunicarse con Juan Mera')
        if marketshare_data:
            st.success("✅ Archivo de Market Share cargado correctamente.")
        else:
            st.info("⏳ Aún falta cargar este archivo.")

with col2:
    with st.container(border=True):
        st.markdown("📁 **Archivo del Padrón (Excel)**")
        padron_data = st.file_uploader("Subí el padrón actualizado", type=["xlsx"], help='Para mas ifnormacion comunicarse con Juan Mera')
        if padron_data:
            st.success("✅ Archivo del padrón cargado correctamente.")
        else:
            st.info("⏳ Aún falta cargar este archivo.")

# ----------- PASO 2: TRANSFORMAR DATOS -----------
st.subheader("Paso 2 - Previsualizar Datos Transformados", divider="rainbow")

if marketshare_data and padron_data:
    st.markdown("✅ Archivos cargados correctamente. Podés transformar los datos para verificar que todo esté bien antes de subir a GCP.")

    if st.button("🔄 Transformar Datos", use_container_width=True, type="primary"):
        with st.spinner("Transformando archivos..."):
            resultado = marketshare(marketshare_data, padron_data)
        
        if isinstance(resultado, dict) and "ventas_df" in resultado and "padron_df" in resultado:
            ventas_df = resultado["ventas_df"]
            padron_df = resultado["padron_df"]

            st.success("✅ Datos transformados exitosamente. A continuación se muestran los primeros registros.")
            
            with st.expander("🧾 Previsualizar - Datos de Market Share"):
                st.dataframe(ventas_df.head(20), use_container_width=True)

            with st.expander("🏪 Previsualizar - Padron Actualizado"):
                st.dataframe(padron_df.head(20), use_container_width=True)

            # ----------- PASO 3: SUBIR DATOS -----------
            st.subheader("Paso 3 - Subida Final a GCP", divider="rainbow")
            st.markdown("Si los datos previsualizados son correctos, podés subirlos a BigQuery.")

            if st.button("🚀 Subir a BigQuery", use_container_width=True):
                with st.spinner("Subiendo a GCP..."):
                    error = subir_a_gcp(ventas_df, padron_df)

                if error is None:
                    st.success("🎉 Datos subidos correctamente a GCP.")
                else:
                    st.error(f"❌ Error al subir los datos: {error}")
        else:
            st.error(f"⚠️ Hubo un error durante la transformación. Detalles: {resultado}")
else:
    st.warning("⚠️ Faltan archivos. Subí ambos archivos para continuar.")

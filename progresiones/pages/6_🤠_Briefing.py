import streamlit as st
from datetime import datetime
from utils.utils import briefing, proteger_pagina
import pandas as pd
import io

st.set_page_config(layout='wide')
proteger_pagina()

st.title('🧾 Generar Briefing Semanal Automático')
st.divider()
st.markdown('Utilizá esta herramienta para generar automáticamente los reportes semanales del Briefing. Subí los archivos descargados desde MicroStrategy y seleccioná el mes de comparabilidad.')

# ---------- SUBIR ARCHIVOS ----------
st.subheader('Paso 1 - Carga de Archivos', divider='rainbow')

col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.markdown('**1. Ventas y Volumen por Tienda (CSV)**')
        ventas_y_volumen = st.file_uploader("📁 Subí archivo de Ventas y Volumen", type=["csv"])

        if ventas_y_volumen:
            st.success("Archivo cargado correctamente")
        else:
            st.info("Falta subir este archivo")


    with st.container(border=True):
        st.markdown('**3. Padrón (XLSX)**')
        padron = st.file_uploader("📁 Subí el Padrón actualizado", type=["xlsx"])

        if padron:
            st.success("Archivo cargado correctamente")
        else:
            st.info("Falta subir este archivo")

    with st.container(border=True):
        st.markdown('**5. Histórico Ventas (CSV)**')
        historico_ventas = st.file_uploader("📁 Subí archivo de Histórico de Ventas", type=["csv"])

        if historico_ventas:
            st.success("Archivo cargado correctamente")
        else:
            st.info("Falta subir este archivo")

with col2:

    with st.container(border=True):
        st.markdown('**2. Débitos por Tienda (CSV)**')
        debitos_por_tienda = st.file_uploader("📁 Subí archivo de Débitos por Tienda", type=["csv"])

        if debitos_por_tienda:
            st.success("Archivo cargado correctamente")
        else:
            st.info("Falta subir este archivo")

    with st.container(border=True):
        st.markdown('**4. Débitos por Sector (CSV)**')
        debitos_por_sector = st.file_uploader("📁 Subí archivo de Débitos por Sector", type=["csv"])

        if debitos_por_sector:
            st.success("Archivo cargado correctamente")
        else:
            st.info("Falta subir este archivo")

    with st.container(border=True):
        st.markdown('**6. Histórico Débitos (CSV)**')
        historico_debitos = st.file_uploader("📁 Subí archivo de Histórico de Débitos", type=["csv"])

        if historico_debitos:
            st.success("Archivo cargado correctamente")
        else:
            st.info("Falta subir este archivo")

with st.container(border=True):
    st.markdown('**7. Histórico Volumen sin Envases (CSV)**')
    historico_volumen = st.file_uploader("📁 Subí archivo de Volumen sin Envases", type=["csv"])

    if historico_volumen:
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

archivos_ok = all([
    ventas_y_volumen,
    debitos_por_tienda,
    padron,
    debitos_por_sector,
    historico_ventas,
    historico_debitos,
    historico_volumen
])

if archivos_ok:
    st.markdown("✅ Todos los archivos fueron cargados correctamente. Ya podés generar el ZIP con los reportes.")

    if st.button("🚀 Generar Briefing Semanal", type="primary", use_container_width=True):
        with st.spinner("⏳ Generando briefing... Tiempo estimado: 1-2 minutos"):
            zip_file = briefing(
                ventas_y_volumen,
                debitos_por_tienda,
                padron,
                debitos_por_sector,
                historico_ventas,
                historico_volumen,
                historico_debitos,
                mes_comparable
            )

            if isinstance(zip_file, bytes) or isinstance(zip_file, io.BytesIO):
                st.success("✅ Briefing generado correctamente.")
                st.download_button(
                    label="📦 Descargar Reporte ZIP",
                    data=zip_file,
                    file_name=f"Briefing Semanal - {mes_comparable}.zip",
                    mime="application/zip",
                    use_container_width=True
                )
            else:
                st.error(f"⚠️ Ocurrió un error: {zip_file}")
else:
    st.warning("⚠️ Faltan archivos por cargar. Subí todos los archivos para poder continuar.")


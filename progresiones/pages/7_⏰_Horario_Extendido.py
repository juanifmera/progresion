import streamlit as st
from datetime import datetime
from utils.utils import analisis_horario_extendido, proteger_pagina
import io

# Configuración inicial
st.set_page_config(layout='wide')
proteger_pagina()

st.title('⏳ Análisis de Horario Extendido - Tiendas Express')
st.divider()
st.markdown(
    'Esta herramienta permite generar un **análisis automatizado** sobre las tiendas Express que modificaron su horario de apertura o cierre los días domingos. '
    'El proceso combina la información de **Ventas por Media Hora**, **Margen**, **Costo de Hora Hombre** y **Horas Autorizadas**, para calcular el **ROC estimado** del horario extendido y determinar su rentabilidad.'
)

st.subheader('Primeros Pasos', divider='rainbow')

col1, col2 = st.columns(2)

# Paso 1 - Ventas por Media Hora
with col1:
    with st.container(border=True):
        st.markdown('**1- PRIMER PASO**: Cargá el archivo de **Ventas por Media Hora** en formato CSV de Micro',
                    help='Reporte de MicroStrategy con ventas acumuladas por media hora, punto operacional y dirección. '
                         'Debe contener datos desde abril hasta la fecha, únicamente de tiendas Express.')
        ventas_por_media_hora = st.file_uploader(
            'Colocar aquí archivo CSV de Ventas por Media Hora', 
            type=['csv'], accept_multiple_files=False
        )
        if ventas_por_media_hora:
            st.success('✅ Archivo de Ventas por Media Hora cargado correctamente.')
        else:
            st.info('Falta cargar archivo.')

    with st.container(border=True):
        st.markdown('**2- SEGUNDO PASO**: Cargá el archivo de **Margen** (formato CSV) desde Tableau',
                    help='Reporte de Margen x Tienda descargado desde Tableau. Debe incluir columnas de "Rubro", "Tienda" y "Importe Ars".')
        margen = st.file_uploader('Colocar aquí archivo de Margen', type=['csv'], accept_multiple_files=False)
        if margen:
            st.success('✅ Archivo de Margen cargado correctamente.')
        else:
            st.info('Falta cargar archivo.')

# Paso 2 - Margen
with col2:
    with st.container(border=True):
        st.markdown('**3- TERCER PASO**: Cargá el archivo de **Costo Horas Hombre** (formato XLSX)',
                    help='Archivo completado con los costos de hora hombre más cargas sociales por tienda y mes. '
                         'Debe contener la hoja llamada "costo_ho".')
        costo_horas = st.file_uploader('Colocar aquí archivo de Costo Horas Hombre', type=['xlsx'], accept_multiple_files=False)
        if costo_horas:
            st.success('✅ Archivo de Costo Horas Hombre cargado correctamente.')
        else:
            st.info('Falta cargar archivo.')

    with st.container(border=True):
        st.markdown('**4- CUARTO PASO**: Cargá el archivo de **Horas Autorizadas** (formato XLSX)',
                    help='Archivo con las horas aprobadas de apertura y cierre de los domingos. '
                         'Debe contener la hoja llamada "horas_dom".')
        horas_autorizadas = st.file_uploader('Colocar aquí archivo de Horas Autorizadas', type=['xlsx'], accept_multiple_files=False)
        if horas_autorizadas:
            st.success('✅ Archivo de Horas Autorizadas cargado correctamente.')
        else:
            st.info('Falta cargar archivo.')

# --- Sección Final ---
st.subheader('Ya casi estamos...', divider='rainbow')
st.markdown(
    'Una vez cargados los cuatro archivos, presioná el botón de abajo para ejecutar la automatización. '
    'El sistema procesará toda la información, realizará los cálculos y generará un archivo Excel con el análisis completo del **horario extendido**.'
)

if ventas_por_media_hora and margen and costo_horas and horas_autorizadas:
    ejecutar = st.button('🚀 Generar Análisis de Horario Extendido', type='primary', use_container_width=True)

    if ejecutar:
        with st.spinner('🔄 Procesando la información y generando el reporte (Tiempo estimado: 1-2 min)...'):
            resultado = analisis_horario_extendido(ventas_por_media_hora, margen, costo_horas, horas_autorizadas)

            if isinstance(resultado, io.BytesIO):
                st.success('✅ Reporte generado correctamente.')
                st.download_button(
                    label='📥 Descargar Análisis en Excel',
                    data=resultado,
                    file_name=f'Horario Extendido Express - {datetime.today().strftime('%d-%m-%Y')}.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    use_container_width=True
                )
            else:
                st.error(resultado)
else:
    st.warning('⚠️ Aún faltan archivos por cargar para ejecutar la automatización.')
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

with st.expander('¡¡¡Revisar Informacion Importante!!!'):
    st.code('''
    1- Colocar archivo de Ventas por Media Hora de Express (CSV).
        a- Tener en cuenta que el rango de fechas debera comenzar desde Abril 2025 hasta el mes corriente de analisis.
        b- El archivo al contener tanta informacion, este tendra un formato "LONG". Si se descarga el informe directamente desde Micro Strategy, asegurarse de quitar la columna "HORA" de la cuadricula, ya que no se necesita.
        c- El Informe se llama "22b. Analisis de Vtas por punto/Seccion/dia/hora (tomadad de 30 min)".
        d- Una vez dentro del reporte colocar los filtros de Año, Mes, Dia, Direccion, Punto Operacional, listar los sectores PGC, PFT, Electro, Textil, Bazar, Otros dentro de Estructura Comercial, Filtrar unicamente las tiendas de Express y Seleccionar el rango de fechas adecuadas desde Abril 2025 hasta el mes corriente que se este haciendo el analisis.

    2- Descargar y colocar el archivo de Margen desde el Tablero de descarga de Margen.
        a- La hoja se llama "TABLERO DESCARGA MARGEN". 
        b- Se deberan utilizar los filtros para reducir el rango del periodo desde Abril 2025 en adelante al mes corriente, el filtro de formato para obtener solamente la informacion de EXPRESS.
        c- Se deberan agrupar las columnas de la tabla para unicamente obtener el detalle cerrado de del rubro. Asimismo, se debera expandir el detalle de la columna formato para obtener el detalle de las tiendas. De esta forma se obtendrá un archivo con la columna del Periodo, Formato, Tienda, Rubro Concat (Grupo) 1, Total Sector e Importe ARS.
        d- Por ultimo y mas importante, para descargar el informe, NO SE DEBE HACER DESDE EL BOTON "Descarga" de la pantalla principal, ya que si lo hacemos por este boton, la columna de valores se exporta sin titulo. Para descargar el margen de forma correcta, se debe ir a la parte superior derecha de Tableau, apretar el icono que parece una pantalla con una flecha hacia abajo, y una vez abierta la pantall de descarga, arriba a la derecha apretar "descargar".

    3- Descargar y colocar el Costo de Horas Hombre y las horas Autorizadas
        a- El detalle de Costo Horas Hombre y Horas Autorizadas Domingo se lo tiene que pedir a Belu o Sebas de Gasto de Personal.
        b- Generado en el drive de Control de Gestion hay una carpeta llamada "23 - Horas Autorizadas y Costo Horas Hombre", alli se encontrara un archivo que contiene toda la informacion necesaria para completar el analisis.
        c- La aplicacion detecta automaticamente de que Hoja extrar la informacion para cada Uploader. Solamente se tiene que asegurar que este completa la columna del mes que se quiere anlizar.
        d- Descargar archivo e incluir el mismo archivo a los dos espacios reservados para cargar las horas autorizadas y el costo de hora hombre.
    ''')

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
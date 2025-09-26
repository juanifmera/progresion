import streamlit as st
from datetime import datetime
from utils.utils import genero_df_comparacion, actualizo_df_comparacion, proteger_pagina

st.set_page_config(layout='wide')
proteger_pagina()

@st.cache_data(show_spinner="Procesando datos y consolidando información...")
def get_df_final(ventas, debitos, padron, mes_comparable: str):
    '''
    Helper cacheado para consolidar la información de progresiones.
    Si los archivos o el mes cambian, recalcula.
    '''
    df_final = genero_df_comparacion(
        ventas=ventas,
        debitos=debitos,
        padron=padron,
        mes_comparable=mes_comparable
    )
    return df_final

st.title('Comparativa de Progresiones vs Formato')
st.divider()
st.markdown('Utiliza esta herramienta para poder comparar las progresiones de una o varias tiendas contra su formato. De esta forma podras visualizar si una tienda performa mejor o peor que el formato en general')

st.subheader('Primeros Pasos: Carga de Informacion', divider='rainbow')

col1, col2, col3 = st.columns(3)

with col1:
    with st.container(border=True):
        st.markdown('**1- PRIMER PASO**: Cargá el archivo de **Ventas y Volumen** (CSV de MicroStrategy)')
        ventas_y_volumen = st.file_uploader('Archivo CSV Ventas y Volumen', type=['csv'])
        if ventas_y_volumen:
            st.success('Archivo Ventas y Volumen cargado correctamente')
        else:
            st.info('Falta cargar archivo de Ventas y Volumen')

with col2:
    with st.container(border=True):
        st.markdown('**2- SEGUNDO PASO**: Cargá el archivo de **Débitos** (CSV de MicroStrategy)')
        debitos = st.file_uploader('Archivo CSV Débitos', type=['csv'])
        if debitos:
            st.success('Archivo Débitos cargado correctamente')
        else:
            st.info('Falta cargar archivo de Débitos')

with col3:
    with st.container(border=True):
        st.markdown('**3- TERCER PASO**: Cargá el archivo del **Padrón** (Excel desde Drive)')
        padron = st.file_uploader('Archivo Excel Padrón', type=['xlsx'])
        if padron:
            st.success('Archivo Padrón cargado correctamente')
        else:
            st.info('Falta cargar archivo de Padrón')


# ================== SEGUNDO BLOQUE: MESES ==================
st.subheader('Segundo Paso: Seleccion de Mes Comparable', divider='rainbow')
st.markdown('Una vez subidos los tres archivos, seleccioná el mes a comparar.')

if ventas_y_volumen and debitos and padron:
    meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
             'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

    # mes actual (si hoy es septiembre -> default agosto)
    mes_actual = meses[datetime.today().month - 2]
    mes = st.selectbox('Elegí un mes para calcular progresiones y acumulados:', meses,
                       index=datetime.today().month - 2, placeholder=mes_actual)

    calculate = st.button('Consolidar información', type='primary', use_container_width=True)

    if calculate:
        # 👇 uso el helper cacheado
        df_final = get_df_final(
            ventas=ventas_y_volumen,
            debitos=debitos,
            padron=padron,
            mes_comparable=mes
        )

        if not isinstance(df_final, Exception) and not df_final.empty:
            st.session_state["df_final"] = df_final
            st.success('✅ Consolidación exitosa')
        else:
            st.error(f"❌ No se pudo generar el DataFrame. {df_final}")


# ================== TERCER BLOQUE: FILTROS Y GRÁFICO ==================
    if "df_final" in st.session_state:
        df_final = st.session_state["df_final"]

        st.subheader('Tercer Paso: Filtros y Gráfico', divider='rainbow')
        st.dataframe(df_final)

        with st.container(border=True):
            col1, col2 = st.columns(2)
            with col1:
                categoria = st.radio('Elegí una categoría:', options=['VCT', 'DEB', 'VOL'], horizontal=True, )

            tiendas_disponibles = sorted(df_final['punto_operacional'].unique().tolist())
            if 'Total Formato' in tiendas_disponibles:
                tiendas_disponibles.remove('Total Formato')

            with col2:
                tiendas = st.multiselect('Elegí tiendas a comparar contra el Formato:',
                                    options=tiendas_disponibles)

        if tiendas and categoria:
            fig = actualizo_df_comparacion(df_final=df_final, tiendas=tiendas, categoria=categoria)
            st.plotly_chart(fig, use_container_width=True)

            if tiendas or categoria:
                st.rerun()

            st.write("Valores únicos en categoria:", df_final["categoria"].unique())
            st.write("Ejemplo de punto_operacional:", df_final["punto_operacional"].unique()[:10])

        else:
            st.info("⚠️ Seleccioná al menos una tienda y una categoría para ver el gráfico.")
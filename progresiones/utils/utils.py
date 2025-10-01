from pathlib import Path
import pandas as pd
from datetime import datetime
import plotly.express as px
import io
import chardet
import streamlit as st
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


def proteger_pagina():
    if "authentication_status" not in st.session_state or st.session_state["authentication_status"] != True:
        st.warning("🔐 Debés iniciar sesión para acceder a esta página.")
        st.stop()

def detectar_encoding(archivo):
    # archivo es un UploadedFile de Streamlit
    raw_data = archivo.read(10000)  # leo los primeros 10k bytes
    archivo.seek(0)  # reseteo el puntero para no perder info
    result = chardet.detect(raw_data)
    return result["encoding"]

def leer_archivo(path_o_buffer, tipo: str, header=None):
    """
    Función genérica para leer CSV, XLSX o Parquet según tipo.
    """
    tipo = tipo.lower()
    if tipo == "csv":
        return pd.read_csv(path_o_buffer, encoding="utf-16", header=header, sep=",", decimal=",")
    elif tipo == "xlsx":
        return pd.read_excel(path_o_buffer, header=header)
    elif tipo == "parquet":
        return pd.read_parquet(path_o_buffer, engine="pyarrow")
    else:
        raise ValueError(f"Formato no soportado: {tipo}")

def progresiones_mmaa(volumen_y_ventas, debitos, padron, mes_comparable:str):
    try:
        '''
        Pipeline para consegur las progresiones de un mes puntual, teniendo en cuenta la SC. Las progresiones se mostraran por Formato, por Tienda, Sector, Seccion, Grupo de Familia y Provincia.

        Los archivos de Ventas y Debitos se deben cargar en formato csv como salen de Micro y el padron en formato xslx (Excel Normal) desde el drive.
        '''
        #Leo el encoding para utilziar esto en computadoras normales
        encoding_vol = detectar_encoding(volumen_y_ventas)
        encoding_deb = detectar_encoding(debitos)

        # Carga de Archivos y transformaciones generales
        df_ventas_y_volumen = pd.read_csv(volumen_y_ventas, encoding='utf-16', header=1)
        df_debitos = pd.read_csv(debitos, encoding='utf-16', header=1, sep=',', decimal=',')
        padron = pd.read_excel(padron, header=17)

        # Trabajo sobre Ventas y Volumen
        #Me quedo unicamente con las columnas importantes
        df_ventas_y_volumen = df_ventas_y_volumen[['Año', 'Mes', 'Direccion', 'Punto Operacional', 'Sector', 'Seccion', 'Grupo de Familia', 'Ventas c/impuesto', 'Venta en Unidades']]

        #Renombro las columnas
        df_ventas_y_volumen.columns = (df_ventas_y_volumen.columns.str.strip().str.lower().str.replace(" ", "_"))
        df_ventas_y_volumen.rename(columns={
            'ventas_c/impuesto':'venta',
            'venta_en_unidades':'volumen'
        }, inplace=True)

        #Genero una columna para Obtener el ID tienda
        df_ventas_y_volumen['numero_operacional'] = df_ventas_y_volumen['punto_operacional'].str.split('-').str[0]

        #Me quedo con las columnas necesarias
        ventas = df_ventas_y_volumen[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'seccion', 'grupo_de_familia', 'venta']]
        volumen = df_ventas_y_volumen[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'seccion', 'grupo_de_familia', 'volumen']]

        #Quito los NA de las columans de valores
        ventas.dropna(subset=['venta'], how='any', inplace=True)
        volumen.dropna(subset=['volumen'], how='any', inplace=True)

        #Realizo transformaciones para quitar carateres y convertir las columnas a valores numericos
        ventas['venta'] = ventas['venta'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
        volumen['volumen'] = volumen['volumen'].str.split(',').str[0].str.replace('.', '', regex=False).astype(int)

        #Renombro las columnas con valores de ambos DF
        ventas.rename(columns={
            'venta':'valores'
        }, inplace=True)

        volumen.rename(columns={
            'volumen':'valores'
        }, inplace=True)

        #Categorizo los valores tanto de volumne como de Ventas
        ventas['categoria'] = 'VCT'
        volumen['categoria'] = 'VOL'

        #Agrupo las ventas
        ventas_agrupado = ventas.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria'])['valores'].sum().reset_index()

        #Quito Envases del Volumen y Agrupo
        volumen_sin_vol = volumen[~volumen['grupo_de_familia'].str.contains('ENVASES')]
        volumen_agrupado = volumen_sin_vol.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria'])['valores'].sum().reset_index()

        # Trabajo sobre Debitos
        # Renombro el DF
        debitos_agrupados = df_debitos

        # Renombro las columnas como corresponden
        debitos_agrupados.columns = debitos_agrupados.columns.str.lower().str.replace(' ','_')
        debitos_agrupados.rename(columns={
            'cant._tickets_por_local':'valores'
        }, inplace=True)

        # Renombro la columna de Debitos a valores
        debitos_agrupados['categoria'] = 'DEB'

        # Genero una columna Categorica
        debitos_agrupados['numero_operacional'] = debitos_agrupados['punto_operacional'].str.split('-').str[0]

        # Genero columna para el ID tienda
        debitos_agrupados = debitos_agrupados[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria', 'valores']]

        # Quito nulos numericos de la columna valores
        debitos_agrupados.dropna(subset=['valores'], how='any', inplace=True)

        # Convierto la columna de valores a su tipo de datos correspondiente
        debitos_agrupados['valores'] = debitos_agrupados['valores'].str.replace('.', '', regex=False).astype(int)

        # Trabajo sobre el padron
        # Selecciono las columnas que me sirven del padron
        padron = padron[['N°', 'NOMBRE', 'Fecha apertura', 'ORGANIZACIÓN ', 'M² SALÓN', 'M² PGC', 'M² PFT', 'M² BAZAR', 'M² Electro', 'M² Textil', 'M² Pls', 'M² GALERIAS', 'PROVINCIA', 'M² Parcking', 'FIN DE CIERRE', 'ENE.2', 'FEB.2', 'MAR.2', 'ABR.2', 'MAY.2', 'JUN.2', 'JUL.2', 'AGO.2', 'SEP.2', 'OCT.2', 'NOV.2', 'DIC.2']]

        # Cambio de nombres en el padron
        padron.columns = (
            padron.columns
            .str.lower()
            .str.strip()
            .str.replace(' ', '_', regex=False)
            .str.replace('m²', 'm', regex=False)
            .str.replace('.2','')
        )

        # Formateo la fecha para que tenga sentido
        padron['fecha_apertura'] = padron['fecha_apertura'].dt.strftime('%d/%m/%Y')

        # Cambio el nombre de la columna N por "Numero Operacional"
        padron.rename(columns={'n°':'numero_operacional'}, inplace=True)

        # Quito los valores nulos utilizando como referencia la columna Numero Operacional, nombre y fecha apertura
        padron.dropna(subset=['numero_operacional', 'nombre', 'fecha_apertura', mes_comparable[0:3].lower()], how='any', inplace=True)

        # Genero una funcion para convertir los valores de una columna a mayuscula
        def maysc(df: pd.DataFrame, columna: str):
            df[columna] = df[columna].astype(str).str.upper()

        maysc(padron, mes_comparable[0:3].lower())
            #Aplico la formula a la columna del mes comparable para que todos los valores sean en mayuscula

        # Coloco el numero operacional como numero
        padron['numero_operacional'] = padron['numero_operacional'].astype(int)


        # Concateno todos los df (venta, debito y volumen) y lo joineo con el padron
        df = pd.concat([ventas_agrupado, volumen_agrupado, debitos_agrupados])

        # Convierto el ID a numero
        df['numero_operacional'] = df['numero_operacional'].astype(int)

        # Genero el Join del df Agupado con el Padron con el objetivo de quedarme unicamente con aquellas tiendas Comparables
        df_join = df.merge(padron, how='left', on='numero_operacional')

        # Trabajo sobre Progresiones Total Formato
        # Me quedo unicamente con las columnas que me sirven del DF Joineado (ACA TENGO LA SC DEL MES)
        df_join = df_join[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'fecha_apertura', 'fin_de_cierre', 'provincia','categoria', 'valores', mes_comparable[0:3].lower()]]

        # Filtro unicamente las lineas que sean Superficie Comparable
        df_join_sc = df_join[df_join[mes_comparable[0:3].lower()] == 'SC']
        df_progresiones_total_carrefour = df_join_sc.groupby(['año','categoria'])['valores'].sum().reset_index().pivot_table('valores', ['categoria'], 'año', 'sum').reset_index()

        # Genero la Columna de Progresiones
        df_progresiones_total_carrefour['progresion'] = round(((df_progresiones_total_carrefour[2025] / df_progresiones_total_carrefour[2024]) - 1) * 100, 1)

        # Trabajo sobre Progresiones por Formato
        # Agrupo el df por año, direccion y categoria para pivotrear y construir un df para realizar las progresiones
        df_progresiones_formato = df_join_sc.groupby(['año', 'direccion','categoria'])['valores'].sum().reset_index().pivot_table('valores', ['direccion', 'categoria'], 'año', 'sum').reset_index()

        # Genero la Columna de Progresiones
        df_progresiones_formato['progresion'] = round(((df_progresiones_formato[2025] / df_progresiones_formato[2024]) - 1) * 100, 1)

        # Pivoteo nuevamente la informacion para que este en un formato mas legible (Wide y no Long)
        df_progresiones_formato = df_progresiones_formato.pivot_table([2024, 2025, 'progresion'], 'direccion', 'categoria').sort_values(by=('progresion', 'VOL'), ascending=False)

        # Trabajo sobre Progresiones por Provincia
        df_progresiones_provincia = df_join_sc.groupby(['año', 'provincia','categoria'])['valores'].sum().reset_index().pivot_table('valores', ['provincia', 'categoria'], 'año', 'sum').reset_index()

        # Genero la Columna de Progresiones
        df_progresiones_provincia['progresion'] = round(((df_progresiones_provincia[2025] / df_progresiones_provincia[2024]) - 1) * 100, 1)

        # Pivoteo nuevamente la informacion para que este en un provincia mas legible (Wide y no Long)
        df_progresiones_provincia = df_progresiones_provincia.pivot_table([2024, 2025, 'progresion'], 'provincia', 'categoria').sort_values(by=('progresion', 'VOL'), ascending=False)

        # Trabajo sobre Progresiones por Tiendas / Formatos
        # Agrupo el DF joineado con el padron y ya con la superficie comparable y agrego las tiendas
        df_progresiones_tiendas = df_join_sc.groupby(['año', 'direccion', 'punto_operacional','categoria'])['valores'].sum().reset_index().pivot_table('valores', ['direccion', 'punto_operacional', 'categoria'], 'año', 'sum').reset_index()

        # Genero las Progresiones
        df_progresiones_tiendas['progresion'] = round(((df_progresiones_tiendas[2025] / df_progresiones_tiendas[2024]) - 1) * 100, 1)

        # Pivoteo la Informacion para mostrar en unformato Wide (Mas legible) y no un un formato long (Mas estructura para trabajar)
        df_progresiones_tiendas = df_progresiones_tiendas.pivot_table(values=[2024, 2025, 'progresion'], index=['direccion', 'punto_operacional'], columns='categoria').reset_index().sort_values(by=['direccion', ('progresion', 'VOL')], ascending=[False, False]) #type:ignore

        # Trabajo sobre Progresiones por Sector Total (Solo Vol y VCT porque Debitos llega hasta el detalle de Tiendas)
        # Concateno las ventas y el volumen sin envases, lo cruzo con el padron, me quedo con los valores comparables segun el mes, genero tres df agrupados por sector, seccion y grupo de familia
        #Concateno las ventas con el volumen sin Envases con el objetivo de agruparlo por sus distintas carecteristicas y  asi conseguir las progresiones totales por Sector, seccion y grupo de familia
        df_venta_volumen = pd.concat([ventas, volumen_sin_vol])

        # Convierto la columna Numero Operacional para realizar el merge con el padron
        df_venta_volumen['numero_operacional'] = df_venta_volumen['numero_operacional'].astype(int) 
        df_venta_volumen = df_venta_volumen.merge(padron, how='left', on='numero_operacional')

        # Me quedo unicamente con las columnas que me sirven y los valores comparables
        df_venta_volumen = df_venta_volumen[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'seccion', 'grupo_de_familia','fecha_apertura', 'fin_de_cierre', 'provincia', 'categoria', 'valores', mes_comparable[0:3].lower()]]
        df_venta_volumen = df_venta_volumen[df_venta_volumen[mes_comparable[0:3].lower()] == 'SC']

        # Agrupo por sector
        df_venta_volumen_agrupado_sector = df_venta_volumen.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'categoria'])['valores'].sum().reset_index()

        # Agrupo por Seccion
        df_venta_volumen_agrupado_seccion = df_venta_volumen.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'seccion', 'categoria'])['valores'].sum().reset_index()

        # Agrupo por grupo de familia
        df_venta_volumen_agrupado_grupo_familia = df_venta_volumen.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'grupo_de_familia', 'categoria'])['valores'].sum().reset_index()

        # Trabajo sobre los Sectores
        # Pivoteo la Info para generar las Progresiones
        sectores_total = df_venta_volumen_agrupado_sector.groupby(['año', 'mes', 'direccion', 'sector', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['sector', 'categoria'], columns='año', aggfunc='sum').reset_index()

        # Genero las progresiones por Sector
        sectores_total['progresion'] = round(((sectores_total[2025] / sectores_total[2024])-1)*100, 1)

        # Pivoteo la Informacion para disponibilizar la informacion en formato wide y no long
        progresion_sectores_total = sectores_total.pivot_table(values=[2024, 2025, 'progresion'], index='sector', columns='categoria', aggfunc='sum').sort_values(by=('progresion', 'VOL'), ascending=False)

        # Trabajo sobre las secciones
        # Pivoteo la Info para generar las Progresiones
        seccion_total = df_venta_volumen_agrupado_seccion.groupby(['año', 'mes', 'direccion', 'seccion', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['seccion', 'categoria'], columns='año', aggfunc='sum').reset_index()

        # Genero las progresiones por seccion
        seccion_total['progresion'] = round(((seccion_total[2025] / seccion_total[2024])-1)*100,1)
        # Pivoteo la Informacion para disponibilizar la informacion en formato wide y no long
        progresion_seccion_total = seccion_total.pivot_table(values=[2024, 2025, 'progresion'], index='seccion', columns='categoria', aggfunc='sum').sort_values(by=('progresion', 'VOL'), ascending=False)

        # Trabajo sobre los Grupos de Familia
        # Pivoteo la Info para generar las Progrgrupo_de_familia
        grupo_de_familia_total = df_venta_volumen_agrupado_grupo_familia.groupby(['año', 'mes', 'direccion', 'grupo_de_familia', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['grupo_de_familia', 'categoria'], columns='año', aggfunc='sum').reset_index()

        # Genero las progresiones por grupo_de_familia
        grupo_de_familia_total['progresion'] = round(((grupo_de_familia_total[2025] / grupo_de_familia_total[2024])-1)*100,1)

        # Pivoteo la Informacion para disponibilizar la informacion en formato wide y no long
        progresion_grupo_de_familia_total = grupo_de_familia_total.pivot_table(values=[2024, 2025, 'progresion'], index='grupo_de_familia', columns='categoria', aggfunc='sum').sort_values(by=('progresion', 'VOL'), ascending=False)

        # Trabajo sobre Ventas y Vol Aperturado por Formato, Tienda, sector, seccion en una misma Tab
        # Agrupo la informacion de las ventas y volumen por sector, seccion y GF. El problema aca es que en una misma tabla no puedo poner subtotales de sector seccion por tienda, por lo que tengo que generar tres tablas diferentes, cada una de estas aperturadas por Tienda y luego (Sector/seccion/GF)
        df_aperturado = df_venta_volumen.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'seccion', 'grupo_de_familia', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', columns='año', index=['direccion', 'punto_operacional', 'sector', 'seccion', 'grupo_de_familia', 'categoria'], aggfunc='sum').reset_index()

        # Obtengo la informacion correspondiente
        df_tienda_sector = df_aperturado.groupby(['direccion', 'punto_operacional', 'sector', 'categoria'])[[2024, 2025]].sum()
        df_tienda_seccion = df_aperturado.groupby(['direccion', 'punto_operacional', 'seccion', 'categoria'])[[2024, 2025]].sum()
        df_tienda_grupo_de_familia = df_aperturado.groupby(['direccion', 'punto_operacional', 'grupo_de_familia', 'categoria'])[[2024, 2025]].sum()

        # Calculo las progresiones
        df_tienda_sector['progresion'] = round(((df_tienda_sector[2025] /df_tienda_sector[2024] -1) *100),1)
        df_tienda_seccion['progresion'] = round(((df_tienda_seccion[2025] /df_tienda_seccion[2024] -1) *100),1)
        df_tienda_grupo_de_familia['progresion'] = round(((df_tienda_grupo_de_familia[2025] /df_tienda_grupo_de_familia[2024] -1) *100),1)

        # Reseteo los index
        df_tienda_sector = df_tienda_sector.reset_index()
        df_tienda_seccion = df_tienda_seccion.reset_index()
        df_tienda_grupo_de_familia = df_tienda_grupo_de_familia.reset_index()

        # Ordeno los valores por su volumen 
        df_tienda_sector = df_tienda_sector.sort_values(by=['direccion', 'punto_operacional', 'sector', 'progresion'], ascending=[False, False, False, False])
        df_tienda_seccion = df_tienda_seccion = df_tienda_seccion.sort_values(by=['direccion', 'punto_operacional', 'seccion', 'progresion'], ascending=[False, False, False, False])
        df_tienda_grupo_de_familia = df_tienda_grupo_de_familia = df_tienda_grupo_de_familia.sort_values(by=['direccion', 'punto_operacional', 'grupo_de_familia', 'progresion'], ascending=[False, False, False, False])

        # Pivoteo para presentar en un formato mas legible
        df_tienda_sector = df_tienda_sector.pivot_table(values=[2024, 2025, 'progresion'], columns='categoria', index=['direccion', 'punto_operacional', 'sector'], aggfunc='sum').reset_index().sort_values(by=[('direccion',''), ('punto_operacional',    ''), ('sector',    ''), ('progresion', 'VOL')], ascending=[False, False, False, False]) #type:ignore

        df_tienda_seccion = df_tienda_seccion.pivot_table(values=[2024, 2025, 'progresion'], columns='categoria', index=['direccion', 'punto_operacional', 'seccion'], aggfunc='sum').reset_index().sort_values(by=[('direccion',''), ('punto_operacional',    ''), ('seccion',    ''), ('progresion', 'VOL')], ascending=[False, False, False, False]) #type:ignore

        df_tienda_grupo_de_familia = df_tienda_grupo_de_familia.pivot_table(values=[2024, 2025, 'progresion'], columns='categoria', index=['direccion', 'punto_operacional', 'grupo_de_familia'], aggfunc='sum').reset_index().sort_values(by=[('direccion',''), ('punto_operacional',    ''), ('grupo_de_familia',    ''), ('progresion', 'VOL')], ascending=[False, False, False, False]) #type:ignore

        try:
            # Exporto todas las tablas a un archivo Excel en memoria
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_progresiones_total_carrefour.to_excel(writer, sheet_name="Prog Carrefour - SC", index=False)
                df_progresiones_formato.to_excel(writer, sheet_name="Prog x Formatos - SC", index=True)
                df_progresiones_provincia.to_excel(writer, sheet_name="Prog x Provincia - SC", index=True)
                df_progresiones_tiendas.to_excel(writer, sheet_name="Prog x Tiendas - SC", index=True)
                progresion_sectores_total.to_excel(writer, sheet_name='Progresiones x Sector - SC', index=True)
                progresion_seccion_total.to_excel(writer, sheet_name='Progresiones x Seccion - SC', index=True)
                progresion_grupo_de_familia_total.to_excel(writer, sheet_name='Progresiones x GF - SC', index=True)
                df_tienda_sector.to_excel(writer, sheet_name='Prog Sector x Tienda - SC', index=True)
                df_tienda_seccion.to_excel(writer, sheet_name='Prog Seccion x Tienda - SC', index=True)
                df_tienda_grupo_de_familia.to_excel(writer, sheet_name='Prog GF x Tienda - SC', index=True)
                df_aperturado.to_excel(writer, sheet_name='Prog Aperturado x Tienda - SC', index=True)

            output.seek(0)
            return output

        except Exception as e:
            print(e)
            return None

    except Exception as e:
        return f'Hubo un error en el medio del flujo/pipeline. Detalle del error: {e}'
    
def exporto_parquet(df: pd.DataFrame):
    try:
        output = io.BytesIO()
        df.to_parquet(output, index=False, engine="pyarrow")
        output.seek(0)
        return output
    
    except Exception as e:
        return f"Ocurrió un error al generar el parquet: {e}"

def progresiones_acumulado(ventas, debitos, padron, mes_comparable:str): 
    try:
        logger.info("🔁 Iniciando cálculo de progresiones acumuladas")

        try:
            ventas.seek(0)
            df_ventas_y_volumen = pd.read_csv(ventas, encoding='utf-16', header=1)
            logger.debug(f"Ventas y Volumen cargado: {df_ventas_y_volumen.shape}")
        except Exception as e:
            logger.error(f'Error leyendo archivo de ventas: {e}')
            return f'Error en ventas. {e}'

        try:
            debitos.seek(0)
            df_debitos = pd.read_csv(debitos, encoding='utf-16', header=1, sep=',', decimal=',')
            logger.debug(f"Debitos cargado: {df_debitos.shape}")
        except Exception as e:
            logger.error(f'Error leyendo archivo de debitos: {e}')
            return f'Error en debitos. {e}'

        try:
            padron.seek(0)
            df_padron = pd.read_excel(padron, header=17)
            logger.debug(f"Padron cargado: {df_padron.shape}")
            padron = df_padron
        except Exception as e:
            logger.error(f'Error leyendo archivo de padron: {e}')
            return f'Error en padron. {e}'

        # Trabajo sobre Ventas y Volumen
        #Me quedo unicamente con las columnas importantes
        df_ventas_y_volumen = df_ventas_y_volumen[['Año', 'Mes', 'Direccion', 'Punto Operacional', 'Sector', 'Seccion', 'Grupo de Familia', 'Ventas c/impuesto', 'Venta en Unidades']].copy()

        #Renombro las columnas
        df_ventas_y_volumen.columns = (df_ventas_y_volumen.columns.str.strip().str.lower().str.replace(" ", "_"))
        df_ventas_y_volumen.rename(columns={
        'ventas_c/impuesto':'venta',
        'venta_en_unidades':'volumen'
        }, inplace=True)

        #Genero una columna para Obtener el ID tienda
        df_ventas_y_volumen['numero_operacional'] = df_ventas_y_volumen['punto_operacional'].str.split('-').str[0]

        #Me quedo con las columnas necesarias
        ventas = df_ventas_y_volumen[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'seccion', 'grupo_de_familia', 'venta']].copy()
        volumen = df_ventas_y_volumen[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'seccion', 'grupo_de_familia', 'volumen']].copy()

        #Quito los NA de las columans de valores
        ventas.dropna(subset=['venta'], how='any', inplace=True)
        volumen.dropna(subset=['volumen'], how='any', inplace=True)

        logger.debug(f"Valores Nulos Quitados")

        #Realizo transformaciones para quitar carateres y convertir las columnas a valores numericos
        ventas['venta'] = ventas['venta'].str.replace('.','').str.replace(',','.').astype('float')
        volumen['volumen'] = volumen['volumen'].str.split(',').str[0].str.replace('.','').astype('int')

        logger.debug(f"Valores transformados a numeros de forma exitosa")

        #Renombro las columnas con valores de ambos DF
        ventas.rename(columns={
        'venta':'valores'
        }, inplace=True)

        volumen.rename(columns={
        'volumen':'valores'
        }, inplace=True)

        #Categorizo los valores tanto de volumne como de Ventas
        ventas['categoria'] = 'VCT'
        volumen['categoria'] = 'VOL'

        logger.debug(f"columnas categorias generadas con exito")

        #Agrupo las ventas
        ventas_agrupado = ventas.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria'])['valores'].sum().reset_index()

        logger.debug(f"Primera agrupacion de ventas {ventas_agrupado.shape}")

        #Quito Envases del Volumen y Agrupo
        volumen_sin_vol = volumen[~volumen['grupo_de_familia'].str.contains('ENVASES')]
        volumen_agrupado = volumen_sin_vol.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria'])['valores'].sum().reset_index()

        logger.debug(f"Se quitaron los envases del volumen y se agrupo el df: {volumen_agrupado.shape}")

        # Trabajo sobre Debitos
        # Renombro el DF
        debitos_agrupados = df_debitos.copy()

        logger.debug(f"debitos cargados {debitos_agrupados.shape}")

        # Renombro las columnas como corresponden
        # Renombro la columna de Debitos a valores
        debitos_agrupados.columns = debitos_agrupados.columns.str.lower().str.replace(' ','_')
        debitos_agrupados.rename(columns={
        'cant._tickets_por_local':'valores'
        }, inplace=True)

        # Genero una columna Categorica
        debitos_agrupados['categoria'] = 'DEB'

        logger.debug(f"Columna categoria para debitos generada con exito")

        # Genero columna para el ID tienda
        debitos_agrupados['numero_operacional'] = debitos_agrupados['punto_operacional'].str.split('-').str[0]

        # Me quedo con las columnas que necesito
        debitos_agrupados = debitos_agrupados[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria', 'valores']].copy()

        logger.debug(f"Agrupo los debitos {debitos_agrupados.shape}")

        # Quito nulos numericos de la columna valores
        debitos_agrupados.dropna(subset=['valores'], how='any', inplace=True)

        logger.debug(f"Quito los nulos de los debitos")

        # Convierto la columna de valores a su tipo de datos correspondiente
        debitos_agrupados['valores'] = debitos_agrupados['valores'].str.replace('.','').astype(int)
        
        logger.debug(f"convierto la columan valores de debitos a numeros")

        # Trabajo sobre el padron
        # Selecciono las columnas que me sirven del padron
        padron = padron[['N°', 'NOMBRE', 'Fecha apertura', 'ORGANIZACIÓN ', 'M² SALÓN', 'M² PGC', 'M² PFT', 'M² BAZAR', 'M² Electro', 'M² Textil', 'M² Pls', 'M² GALERIAS', 'PROVINCIA', 'M² Parcking', 'FIN DE CIERRE', 'ENE.2', 'FEB.2', 'MAR.2', 'ABR.2', 'MAY.2', 'JUN.2', 'JUL.2', 'AGO.2', 'SEP.2', 'OCT.2', 'NOV.2', 'DIC.2']].copy() #type:ignore

        logger.debug(f"cargo el padron {padron.shape}")

        # Cambio de nombres en el padron
        padron.columns = (
        padron.columns
        .str.lower()
        .str.strip()
        .str.replace(' ', '_', regex=False)
        .str.replace('m²', 'm', regex=False)
        .str.replace('.2','')
        )

        # Formateo la fecha para que tenga sentido
        padron['fecha_apertura'] = padron['fecha_apertura'].dt.strftime('%d/%m/%Y')

        # Cambio el nombre de la columna N por "Numero Operacional"
        padron.rename(columns={'n°':'numero_operacional'}, inplace=True)

        # Quito los valores nulos utilizando como referencia la columna Numero Operacional, nombre y fecha apertura
        padron.dropna(subset=['numero_operacional', 'nombre', 'fecha_apertura', mes_comparable[0:3].lower()], how='any', inplace=True)

        # Genero una funcion para convertir los valores de una columna a mayuscula
        def maysc(df: pd.DataFrame, columna: str):
            df[columna] = df[columna].str.upper()

        #Aplico la formula a la columna del mes comparable para que todos los valores sean en mayuscula
        maysc(padron, mes_comparable[0:3].lower())

        # Coloco el numero operacional como numero
        padron['numero_operacional'] = padron['numero_operacional'].astype(int)

        # Concateno todos los df (venta, debito y volumen) y lo joineo con el padron
        df = pd.concat([ventas_agrupado, volumen_agrupado, debitos_agrupados])

        logger.debug(f"Concateno todos los df, ventas, debitos y vol {df.shape}")

        # Convierto el ID a numero
        df['numero_operacional'] = df['numero_operacional'].astype(int)

        # Genero el Join del df Agupado con el Padron con el objetivo de quedarme unicamente con aquellas tiendas Comparables
        df_join = df.merge(padron, how='left', on='numero_operacional')

        logger.debug(f"Genero un Join con el padron {df_join.shape}")

        # Trabajo sobre Progresiones Total Formato
        # Me quedo unicamente con las columnas que me sirven del DF Joineado (ACA TENGO LA SC DEL MES)
        df_join = df_join[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'fecha_apertura', 'fin_de_cierre', 'provincia','categoria', 'valores', mes_comparable[0:3].lower()]].copy()

        logger.debug(f"Join final con padrón: {df_join.shape}, columnas: {df_join.columns.tolist()}")

        #Renombro la Columna Mes a Fecha para Luego generar la Columna Mes Correspondiente
        df_join.rename(columns={
            'mes':'fecha'
        }, inplace=True)
        df_join['mes'] = df_join['fecha'].str.split(' ').str[0]

        # Filtro unicamente las lineas que sean Superficie Comparable
        df_join_sc = df_join[df_join[mes_comparable[0:3].lower()] == 'SC'].copy()

        logger.debug(f"Me quedo unicamente con valores comparables {df_join_sc.shape}")

        #Agrupo el df por categoria teniendo en cuenta el mes, ya que este me servirá luego para limitar el periodo comparable y la superficie comparable
        df_acum_formato = df_join_sc.groupby(['año', 'mes', 'direccion', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['mes', 'categoria'], columns='año', aggfunc='sum').reset_index()

        logger.info("🔄 Generando acumulado a nivel Formato")
        logger.debug(f"Antes del pivot, DF: {df_join_sc.shape}")

        #Genero un diccionario con los meses y sus valores numericos de forma auxiliar
        orden_meses = {"Enero":1, "Febrero":2, "Marzo":3, "Abril":4, "Mayo":5, "Junio":6, "Julio":7, "Agosto":8, "Septiembre":9, "Octubre":10, "Noviembre":11, "Diciembre":12}

        #Genero una columna auziliar para ordenar los meses y luego limitar el periodo
        df_acum_formato['aux'] = df_acum_formato['mes'].map(orden_meses)

        #Limito el periodo del df al mes comparable que quiero
        mes_limite = orden_meses[mes_comparable.capitalize()]
        df_acum_formato = df_acum_formato.loc[df_acum_formato['aux'] <= mes_limite]

        #Vuelvo a ordenar los meses
        df_acum_formato = df_acum_formato.sort_values('aux', ascending=True)

        #Una vez que tengo limitado el df por los meses que me interesan, agrupo el df para quitar el detalle de los meses ya que lo que queremos obtener es la sumatoria de los debitos, ventas y volumen del periodo acumulado indicado
        df_acum_formato = df_acum_formato.groupby(['categoria'])[[2024, 2025]].sum().reset_index()

        logger.debug(f"Agrupo por año quitando el detalle de los meses: {df_acum_formato.shape}")

        #Calculo la Progresion
        df_acum_formato['progresion'] = round((((df_acum_formato[2025] / df_acum_formato[2024]) - 1) * 100), 1)

        logger.debug(f"Calculo la primer progresion a nivel formato")

        ### Trabajo sobre las provincias
        #Agrupo el df por categoria teniendo en cuenta el mes, ya que este me servirá luego para limitar el periodo comparable y la superficie comparable
        df_acum_provincia = df_join_sc.groupby(['año', 'mes', 'direccion', 'provincia', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['mes', 'categoria', 'provincia'], columns='año', aggfunc='sum').reset_index()

        logger.info("🔄 Generando acumulado a nivel Provincia")
        logger.debug(f"Shape antes del pivot provincia: {df_acum_provincia.shape}")

        #Genero una columna auziliar para ordenar los meses y luego limitar el periodo
        df_acum_provincia['aux'] = df_acum_provincia['mes'].map(orden_meses)

        #Limito el periodo del df al mes comparable que quiero
        df_acum_provincia = df_acum_provincia.loc[df_acum_provincia['aux'] <= mes_limite].copy()

        # Una vez que tengo el periodo, ya el mes no me sirve, por eso agrupo por provincia y categoria
        df_acum_provincia = df_acum_provincia.groupby(['categoria', 'provincia'])[[2024, 2025]].sum().reset_index()

        #calculo las Progresiones
        df_acum_provincia['progresion'] = round((((df_acum_provincia[2025] / df_acum_provincia[2024]) - 1) * 100), 1)

        #Pivot para mostrar mejora la info
        df_acum_provincia = df_acum_provincia.pivot_table(values=[2024, 2025, 'progresion'], columns='categoria', index='provincia', aggfunc='sum').sort_values(('progresion', 'VOL'), ascending=False)

        logger.debug(f"Trabajo sobre el df Provincia {df_acum_provincia.shape}")

        ### Trabajo sobre las tiendas
        df_acum_tiendas = df_join_sc.groupby(['año', 'mes', 'direccion', 'categoria', 'punto_operacional'])['valores'].sum().reset_index().pivot_table(values='valores', index=['mes', 'categoria', 'punto_operacional'], columns='año', aggfunc='sum').reset_index()

        logger.info("🔄 Generando acumulado a nivel Tiendas")
        logger.debug(f"Shape antes del pivot provincia: {df_acum_tiendas.shape}")

        #Genero una columna auziliar para ordenar los meses y luego limitar el periodo
        df_acum_tiendas['aux'] = df_acum_tiendas['mes'].map(orden_meses)

        #Limito el periodo del df al mes comparable que quiero
        df_acum_tiendas = df_acum_tiendas.loc[df_acum_tiendas['aux'] <= mes_limite].copy()

        #Vuelvo a ordenar los meses
        df_acum_tiendas = df_acum_tiendas.sort_values('aux', ascending=True)

        #Una vez que tengo limitado el df por los meses que me interesan, agrupo el df para quitar el detalle de los meses ya que lo que queremos obtener es la sumatoria de los debitos, ventas y volumen del periodo acumulado indicado
        df_acum_tiendas = df_acum_tiendas.groupby(['punto_operacional', 'categoria'])[[2024, 2025]].sum().reset_index()

        #Calculo la Progresion
        df_acum_tiendas['progresion'] = round((((df_acum_tiendas[2025] / df_acum_tiendas[2024]) - 1) * 100), 1)
        df_acum_tiendas = df_acum_tiendas.pivot_table(values=[2024, 2025, 'progresion'], columns='categoria', index='punto_operacional', aggfunc='sum').sort_values(('progresion', 'VOL'), ascending=False)

        logger.debug(f"Trabajo sobre las prog acum por tienda {df_acum_tiendas.shape}")

        ### Trabajo con VOL y VCT por Sector, Seccion y GF
        #Concateno las ventas con el volumen sin Envases con el objetivo de agruparlo por sus distintas carecteristicas y  asi conseguir las progresiones totales por Sector, seccion y grupo de familia
        acumulado_venta_volumen = pd.concat([ventas, volumen_sin_vol])

        logger.info("🔄 Generando concat de VOL y VCT Solamente")
        logger.debug(f"Shape antes del concat: {acumulado_venta_volumen.shape}")

        #Convierto la columna Numero Operacional para realizar el merge con el padron
        acumulado_venta_volumen['numero_operacional'] = acumulado_venta_volumen['numero_operacional'].astype(int) 
        acumulado_venta_volumen = acumulado_venta_volumen.merge(padron, how='left', on='numero_operacional')

        logger.info("🔄 Generando Join de la venta y el vol con el padron")

        #Me quedo unicamente con las columnas que me sirven y los valores comparables
        acumulado_venta_volumen = acumulado_venta_volumen[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'seccion', 'grupo_de_familia','fecha_apertura', 'fin_de_cierre', 'provincia', 'categoria', 'valores', mes_comparable[0:3].lower()]]
        acumulado_venta_volumen = acumulado_venta_volumen[acumulado_venta_volumen[mes_comparable[0:3].lower()] == 'SC'].copy() 

        #Renomrbo la columna mes a fecha y genero la columna de mes correcta
        acumulado_venta_volumen.rename(columns={
            'mes':'fecha'
        }, inplace=True)
        acumulado_venta_volumen['mes'] = acumulado_venta_volumen['fecha'].str.split(' ').str[0]

        #Genero una columna auziliar para ordenar los meses y luego limitar el periodo
        acumulado_venta_volumen['aux'] = acumulado_venta_volumen['mes'].map(orden_meses)

        #Limito el periodo del df al mes comparable que quiero
        acumulado_venta_volumen = acumulado_venta_volumen.loc[acumulado_venta_volumen['aux'] <= mes_limite].copy()

        #Vuelvo a ordenar los meses
        acumulado_venta_volumen = acumulado_venta_volumen.sort_values('aux', ascending=True)

        #Agrupo y trabajo por Sector
        acumulado_venta_volumen_sector = acumulado_venta_volumen.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'categoria'])['valores'].sum().reset_index()

        logger.info("🔄 Agrupo por Sector Ventas y VOL")

        #Pivoteo la Info para generar las Progresiones
        acumulado_venta_volumen_sector = acumulado_venta_volumen_sector.groupby(['año', 'mes', 'direccion', 'sector', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['sector', 'categoria'], columns='año', aggfunc='sum').reset_index()

        logger.info("🔄 Pivot VOL Y VCT por Sector")

        #Genero la Progresion
        acumulado_venta_volumen_sector['progresion'] = round(((acumulado_venta_volumen_sector[2025] / acumulado_venta_volumen_sector[2024])-1)*100,1)

        logger.info("🔄 Genero Progresiones")

        #Pivoteo la Informacion para disponibilizar la informacion en formato wide y no long
        acumulado_venta_volumen_sector = acumulado_venta_volumen_sector.pivot_table(values=[2024, 2025, 'progresion'], index='sector', columns='categoria', aggfunc='sum').sort_values(by=('progresion', 'VOL'), ascending=False)

        ### Agrupo y Trabajo por Seccion
        acumulado_venta_volumen_seccion = acumulado_venta_volumen.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'seccion', 'categoria'])['valores'].sum().reset_index()

        #Pivoteo la Info para generar las Progresiones
        acumulado_venta_volumen_seccion = acumulado_venta_volumen_seccion.groupby(['año', 'mes', 'direccion', 'seccion', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['seccion', 'categoria'], columns='año', aggfunc='sum').reset_index()

        #Genero la Progresion
        acumulado_venta_volumen_seccion['progresion'] = round(((acumulado_venta_volumen_seccion[2025] / acumulado_venta_volumen_seccion[2024])-1)*100,1)

        #Pivoteo la Informacion para disponibilizar la informacion en formato wide y no long
        acumulado_venta_volumen_seccion = acumulado_venta_volumen_seccion.pivot_table(values=[2024, 2025, 'progresion'], index='seccion', columns='categoria', aggfunc='sum').sort_values(by=('progresion', 'VOL'), ascending=False)

        logger.info("🔄 Finalizo las secciones")

        ### Agrupo y trabajo por grupo de familia
        acumulado_venta_volumen_grupo_de_familia = acumulado_venta_volumen.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'grupo_de_familia', 'categoria'])['valores'].sum().reset_index()

        #Pivoteo la Info para generar las Progresiones
        acumulado_venta_volumen_grupo_de_familia = acumulado_venta_volumen_grupo_de_familia.groupby(['año', 'mes', 'direccion', 'grupo_de_familia', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['grupo_de_familia', 'categoria'], columns='año', aggfunc='sum').reset_index()

        #Genero la Progresion
        acumulado_venta_volumen_grupo_de_familia['progresion'] = round(((acumulado_venta_volumen_grupo_de_familia[2025] / acumulado_venta_volumen_grupo_de_familia[2024])-1)*100,1)

        #Pivoteo la Informacion para disponibilizar la informacion en formato wide y no long
        acumulado_venta_volumen_grupo_de_familia = acumulado_venta_volumen_grupo_de_familia.pivot_table(values=[2024, 2025, 'progresion'], index='grupo_de_familia', columns='categoria', aggfunc='sum').sort_values(by=('progresion', 'VOL'), ascending=False)

        logger.info("🔄 Finalizo los Grupos de Familia")

        #Agrupo y trabajo por Tienda / Sector
        acumulado_venta_volumen_tienda_sector = acumulado_venta_volumen.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'categoria'])['valores'].sum().reset_index()

        #Pivoteo la Info para generar las Progresiones
        acumulado_venta_volumen_tienda_sector = acumulado_venta_volumen_tienda_sector.groupby(['año', 'mes', 'direccion', 'punto_operacional', 'sector', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['sector', 'punto_operacional', 'categoria'], columns='año', aggfunc='sum').reset_index()

        #Genero la Progresion
        acumulado_venta_volumen_tienda_sector['progresion'] = round(((acumulado_venta_volumen_tienda_sector[2025] / acumulado_venta_volumen_tienda_sector[2024])-1)*100,1)

        #Pivoteo la Informacion para disponibilizar la informacion en formato wide y no long
        acumulado_venta_volumen_tienda_sector = acumulado_venta_volumen_tienda_sector.pivot_table(values=[2024, 2025, 'progresion'], index=['punto_operacional', 'sector'], columns='categoria', aggfunc='sum').sort_values(by=('progresion', 'VOL'), ascending=False).reset_index()

        logger.info("🔄 Finalizo los sectores por Tienda")

        #Agrupo y trabajo por Tienda / Seccion
        acumulado_venta_volumen_tienda_seccion = acumulado_venta_volumen.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'seccion', 'categoria'])['valores'].sum().reset_index()

        #Pivoteo la Info para generar las Progresiones
        acumulado_venta_volumen_tienda_seccion = acumulado_venta_volumen_tienda_seccion.groupby(['año', 'mes', 'direccion', 'punto_operacional', 'seccion', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['seccion', 'punto_operacional', 'categoria'], columns='año', aggfunc='sum').reset_index()

        #Genero la Progresion
        acumulado_venta_volumen_tienda_seccion['progresion'] = round(((acumulado_venta_volumen_tienda_seccion[2025] / acumulado_venta_volumen_tienda_seccion[2024])-1)*100,1)

        #Pivoteo la Informacion para disponibilizar la informacion en formato wide y no long
        acumulado_venta_volumen_tienda_seccion = acumulado_venta_volumen_tienda_seccion.pivot_table(values=[2024, 2025, 'progresion'], index=['punto_operacional', 'seccion'], columns='categoria', aggfunc='sum').sort_values(by=('progresion', 'VOL'), ascending=False).reset_index()

        logger.info("🔄 Finalizo las secciones por Tienda")

        #Agrupo y trabajo por Tienda / GF
        acumulado_venta_volumen_tienda_grupo_de_familia = acumulado_venta_volumen.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'grupo_de_familia', 'categoria'])['valores'].sum().reset_index()

        #Pivoteo la Info para generar las Progresiones
        acumulado_venta_volumen_tienda_grupo_de_familia = acumulado_venta_volumen_tienda_grupo_de_familia.groupby(['año', 'mes', 'direccion', 'punto_operacional', 'grupo_de_familia', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['grupo_de_familia', 'punto_operacional', 'categoria'], columns='año', aggfunc='sum').reset_index()

        #Genero la Progresion
        acumulado_venta_volumen_tienda_grupo_de_familia['progresion'] = round(((acumulado_venta_volumen_tienda_grupo_de_familia[2025] / acumulado_venta_volumen_tienda_grupo_de_familia[2024])-1)*100,1)

        #Pivoteo la Informacion para disponibilizar la informacion en formato wide y no long
        acumulado_venta_volumen_tienda_grupo_de_familia = acumulado_venta_volumen_tienda_grupo_de_familia.pivot_table(values=[2024, 2025, 'progresion'], index=['punto_operacional', 'grupo_de_familia'], columns='categoria', aggfunc='sum').sort_values(by=('progresion', 'VOL'), ascending=False).reset_index()

        logger.info("🔄 Finalizo los Grupos de Familia por Tienda")

        #Aperturo para dejar toda la informacion lista para que el usuario realice una tabla Pivot y tenga todo de forma  compacta
        if df_join_sc['direccion'].unique()[0] != 'PROXIMIDAD':
            acumulado_venta_volumen_total = acumulado_venta_volumen.groupby(['año', 'mes', 'direccion', 'punto_operacional', 'sector', 'seccion', 'grupo_de_familia', 'categoria'])['valores'].sum().reset_index()

        logger.debug(f"Uso de memoria previo al ExcelWriter: {round(df.memory_usage(deep=True).sum() / 1024 ** 2, 2)} MB")

        try:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                logger.info("💾 Comenzando a escribir Excel en memoria")
                df_acum_formato.to_excel(writer, sheet_name=f"Prog Acum {df_join_sc['direccion'].unique()[0]} - SC", index=True)
                df_acum_provincia.to_excel(writer, sheet_name="Prog Acum Provincia - SC", index=True)
                df_acum_tiendas.to_excel(writer, sheet_name="Prog Acum Tiendas - SC", index=True)
                acumulado_venta_volumen_sector.to_excel(writer, sheet_name="Prog Acum Sector - SC", index=True)
                acumulado_venta_volumen_seccion.to_excel(writer, sheet_name="Prog Acum Seccion - SC", index=True)
                acumulado_venta_volumen_grupo_de_familia.to_excel(writer, sheet_name="Prog Acum GF - SC", index=True)
                acumulado_venta_volumen_tienda_sector.to_excel(writer, sheet_name="Prog Sector x Tienda - SC", index=True)
                acumulado_venta_volumen_tienda_seccion.to_excel(writer, sheet_name="Prog Seccion x Tienda - SC", index=True)
                acumulado_venta_volumen_tienda_grupo_de_familia.to_excel(writer, sheet_name="Prog GF x Tienda - SC", index=True)
                #En caso de que el formato sea Express, no se genera la ultima tab para que no se rompa el programa
                if df_join_sc['direccion'].unique()[0] != 'PROXIMIDAD':
                    acumulado_venta_volumen_total.to_excel(writer, sheet_name="Prog Aperturado x Tienda - SC", index=True)

            output.seek(0)
            logger.info("✅ Excel generado correctamente")
            return output

        except Exception as e:
            return f'Error a la hora de guardar excel en memoria. Error: {e}'

    except Exception as e:
        return f'Error a la hora de generar calculos. Error: {e}'
    
def genero_df_comparacion(ventas, debitos, padron, mes_comparable:str):
    try:
        # Carga de Archivos y transformaciones generales
        #TENER CUIDADO A LA HORA DE SUBIR LA INFORMACION. EN ESTE CASO COMO VAMOS A REALIZAR UNA COMPARACION GENERAL POR TIENDA/FORMATO, NO ES NECESARIO APERTURAR EL REPORTE DE VENTAS Y VOLUMEN POR SECTOR SECCION. UNICAMENTE POR GF PARA QUITARLE LOS ENVASES AL VOLUMEN
        df_ventas_y_volumen = pd.read_csv(ventas, encoding='utf-16', header=1)
        df_debitos = pd.read_csv(debitos, encoding='utf-16', header=1, sep=',', decimal=',')
        padron = pd.read_excel(padron, header=17) #type:ignore

        # Trabajo sobre Ventas y Volumen
        #Me quedo unicamente con las columnas importantes
        df_ventas_y_volumen = df_ventas_y_volumen[['Año', 'Mes', 'Direccion', 'Punto Operacional', 'Grupo de Familia', 'Ventas c/impuesto', 'Venta en Unidades']]

        #Renombro las columnas
        df_ventas_y_volumen.columns = (df_ventas_y_volumen.columns.str.strip().str.lower().str.replace(" ", "_"))
        df_ventas_y_volumen.rename(columns={
        'ventas_c/impuesto':'venta',
        'venta_en_unidades':'volumen'
        }, inplace=True)

        #Genero una columna para Obtener el ID tienda
        df_ventas_y_volumen['numero_operacional'] = df_ventas_y_volumen['punto_operacional'].str.split('-').str[0]

        #Me quedo con las columnas necesarias
        ventas = df_ventas_y_volumen[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'grupo_de_familia', 'venta']]
        volumen = df_ventas_y_volumen[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'grupo_de_familia', 'volumen']]

        #Quito los NA de las columans de valores
        ventas.dropna(subset=['venta'], how='any', inplace=True)
        volumen.dropna(subset=['volumen'], how='any', inplace=True)

        #Realizo transformaciones para quitar carateres y convertir las columnas a valores numericos
        ventas['venta'] = ventas['venta'].str.replace('.','').str.replace(',','.').astype('float')
        volumen['volumen'] = volumen['volumen'].str.split(',').str[0].str.replace('.','').astype('int')

        #Renombro las columnas con valores de ambos DF
        ventas.rename(columns={
        'venta':'valores'
        }, inplace=True)

        volumen.rename(columns={
        'volumen':'valores'
        }, inplace=True)

        #Categorizo los valores tanto de volumne como de Ventas
        ventas['categoria'] = 'VCT'
        volumen['categoria'] = 'VOL'

        #Agrupo las ventas
        ventas_agrupado = ventas.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria'])['valores'].sum().reset_index()

        #Quito Envases del Volumen y Agrupo
        volumen_sin_vol = volumen[~volumen['grupo_de_familia'].str.contains('ENVASES')]
        volumen_agrupado = volumen_sin_vol.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria'])['valores'].sum().reset_index()

        # Trabajo sobre Debitos
        # Renombro el DF
        debitos_agrupados = df_debitos

        # Renombro las columnas como corresponden
        debitos_agrupados.columns = debitos_agrupados.columns.str.lower().str.replace(' ','_')

        # Renombro la columna de Debitos a valores
        debitos_agrupados.rename(columns={
        'cant._tickets_por_local':'valores'
        }, inplace=True)

        # Genero una columna Categorica
        debitos_agrupados['categoria'] = 'DEB'

        # Genero columna para el ID tienda
        debitos_agrupados['numero_operacional'] = debitos_agrupados['punto_operacional'].str.split('-').str[0]

        # Ordeno las columnas del df
        debitos_agrupados = debitos_agrupados[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria', 'valores']]

        # Quito nulos numericos de la columna valores
        debitos_agrupados.dropna(subset=['valores'], how='any', inplace=True)

        # Convierto la columna de valores a su tipo de datos correspondiente
        debitos_agrupados['valores'] = debitos_agrupados['valores'].str.replace('.','').astype(int)

        # Trabajo sobre el padron
        # Selecciono las columnas que me sirven del padron
        padron = padron[['N°', 'NOMBRE', 'Fecha apertura', 'ORGANIZACIÓN ', 'M² SALÓN', 'M² PGC', 'M² PFT', 'M² BAZAR', 'M² Electro', 'M² Textil', 'M² Pls', 'M² GALERIAS', 'PROVINCIA', 'M² Parcking', 'FIN DE CIERRE', 'ENE.2', 'FEB.2', 'MAR.2', 'ABR.2', 'MAY.2', 'JUN.2', 'JUL.2', 'AGO.2', 'SEP.2', 'OCT.2', 'NOV.2', 'DIC.2']] #type:ignore

        # Cambio de nombres en el padron
        padron.columns = (
        padron.columns
        .str.lower()
        .str.strip()
        .str.replace(' ', '_', regex=False)
        .str.replace('m²', 'm', regex=False)
        .str.replace('.2','')
        )

        # Formateo la fecha para que tenga sentido
        padron['fecha_apertura'] = padron['fecha_apertura'].dt.strftime('%d/%m/%Y')

        # Cambio el nombre de la columna N por "Numero Operacional"
        padron.rename(columns={'n°':'numero_operacional'}, inplace=True)

        # Quito los valores nulos utilizando como referencia la columna Numero Operacional, nombre y fecha apertura
        padron.dropna(subset=['numero_operacional', 'nombre', 'fecha_apertura', mes_comparable[0:3].lower()], how='any', inplace=True)

        # Genero una funcion para convertir los valores de una columna a mayuscula
        def maysc(df: pd.DataFrame, columna: str):
            df[columna] = df[columna].str.upper()
        #Aplico la formula a la columna del mes comparable para que todos los valores sean en mayuscula

        maysc(padron, mes_comparable[0:3].lower())

        # Coloco el numero operacional como numero
        padron['numero_operacional'] = padron['numero_operacional'].astype(int)

        # Concateno todos los df (venta, debito y volumen) y lo joineo con el padron
        df = pd.concat([ventas_agrupado, volumen_agrupado, debitos_agrupados])

        # Convierto el ID a numero
        df['numero_operacional'] = df['numero_operacional'].astype(int)

        # Genero el Join del df Agupado con el Padron con el objetivo de quedarme unicamente con aquellas tiendas Comparables
        df_join = df.merge(padron, how='left', on='numero_operacional')

        # Trabajo sobre Progresiones Total Formato
        # Me quedo unicamente con las columnas que me sirven del DF Joineado (ACA TENGO LA SC DEL MES)
        df_join = df_join[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'fecha_apertura', 'fin_de_cierre', 'provincia','categoria', 'valores', mes_comparable[0:3].lower()]]

        # Filtro unicamente las lineas que sean Superficie Comparable
        df_join_sc = df_join[df_join[mes_comparable[0:3].lower()] == 'SC']

        #Renombro la Columna Mes a Fecha para Luego generar la Columna Mes Correspondiente
        df_join_sc.rename(columns={
        'mes':'fecha'
        }, inplace=True)
        df_join_sc['mes'] = df_join_sc['fecha'].str.split(' ').str[0]
        
        #Genero un diccionario con los meses y sus valores numericos de forma auxiliar
        orden_meses = {"Enero":1, "Febrero":2, "Marzo":3, "Abril":4, "Mayo":5, "Junio":6, "Julio":7, "Agosto":8, "Septiembre":9, "Octubre":10, "Noviembre":11, "Diciembre":12}

        #Genero una columna auziliar para ordenar los meses y luego limitar el periodo
        df_join_sc['aux'] = df_join_sc['mes'].map(orden_meses)

        # Agrupo por los campos que me sirven y pivoteo la info para calcular las progresiones
        df_total_formato = df_join_sc.groupby(['año', 'mes', 'aux', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['mes', 'aux', 'categoria'], columns='año', aggfunc='sum').reset_index()

        #Genero las progresiones
        df_total_formato['progresion 2024'] = round((((df_total_formato[2024] / df_total_formato[2023]) - 1) * 100), 1)
        df_total_formato['progresion 2025'] = round((((df_total_formato[2025] / df_total_formato[2024]) - 1) * 100), 1)

        #Me traigo unicamente la informacion que me sirve
        df_total_formato = df_total_formato[['mes', 'aux', 'categoria', 'progresion 2024', 'progresion 2025']]

        #Ordeno el df
        df_total_formato = df_total_formato.sort_values('aux')

        #"Derrito" el df para poder generar un formato long y asi realizar un grafico con el periodo continuado desde 2024 en adelante
        df_total_formato = df_total_formato.melt(id_vars=['mes', 'aux', 'categoria'], value_vars=['progresion 2024', 'progresion 2025'], var_name='progresiones', value_name='valores')

        #genero una columna de periodo concatenando el mes y el año
        df_total_formato['periodo'] = df_total_formato['aux'].astype(str).str.zfill(2) + '-' + df_total_formato['progresiones'].str.split(' ').str[1].astype(str).str[2:]

        df_total_formato['punto_operacional'] = 'Total Formato'

        df_total_formato = df_total_formato[['punto_operacional', 'mes', 'aux', 'categoria', 'progresiones', 'valores', 'periodo']]

        # Agrupo por los campos que me sirven y pivoteo la info para calcular las progresiones
        df_join_sc = df_join_sc.groupby(['año', 'mes', 'aux', 'punto_operacional', 'categoria'])['valores'].sum().reset_index().pivot_table(values='valores', index=['mes', 'aux', 'punto_operacional', 'categoria'], columns='año', aggfunc='sum').reset_index()

        #Genero las progresiones
        df_join_sc['progresion 2024'] = round((((df_join_sc[2024] / df_join_sc[2023]) - 1) * 100), 1)
        df_join_sc['progresion 2025'] = round((((df_join_sc[2025] / df_join_sc[2024]) - 1) * 100), 1)

        #Me traigo unicamente la informacion que me sirve
        df_progresiones = df_join_sc[['punto_operacional', 'mes', 'aux', 'categoria', 'progresion 2024', 'progresion 2025']]

        #Ordeno el df
        df_progresiones = df_progresiones.sort_values('aux')

        #"Derrito" el df para poder generar un formato long y asi realizar un grafico con el periodo continuado desde 2024 en adelante
        df_progresiones = df_progresiones.melt(id_vars=['punto_operacional', 'mes', 'aux', 'categoria'], value_vars=['progresion 2024', 'progresion 2025'], var_name='progresiones', value_name='valores')

        #genero una columna de periodo concatenando el mes y el año
        df_progresiones['periodo'] = df_progresiones['aux'].astype(str).str.zfill(2) + '-' + df_progresiones['progresiones'].str.split(' ').str[1].astype(str).str[2:]

        #Concateno los dos dfs finales
        df_final = pd.concat([df_progresiones, df_total_formato])

        return df_final

    except Exception as e:
        return e
    
def actualizo_df_comparacion(df_final:pd.DataFrame, tiendas:list, categoria:str):
        try:
            #Genero un df por tienda seleccionada (IMPORTANTE CAMBIAR ACA POR PARAMETROS EN LA FUNCION Y QUE SE ACTUALICE EN BASE A ESTO)
            df_final_filtrado = df_final[(df_final['punto_operacional'].isin(['Total Formato'] + tiendas)) & (df_final['categoria'] == categoria)]

            fig = px.line(
                df_final_filtrado,
                x="periodo",
                y="valores",
                markers=True,
                text="valores",
                color='punto_operacional'
            )

            fig.update_layout(
                title={
                    'text': f"Evolución {df_final_filtrado['categoria'].unique()[0]} vs Formato",
                    'y':0.95,
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                yaxis_title="Valores en porcentaje",
                xaxis_title="Periodo (Mes-Año)"
            )

            fig.update_traces(textposition="top center")

            return fig
        
        except Exception as e:
            return e
        
def obtener_join(ventas, debitos, padron, mes_comparable:str): 
    try:
        # Carga de Archivos y transformaciones generales
        df_ventas_y_volumen = pd.read_csv(ventas, encoding='utf-16', header=1)
        df_debitos = pd.read_csv(debitos, encoding='utf-16', header=1, sep=',', decimal=',')
        padron = pd.read_excel(padron, header=17) #type:ignore

        # Trabajo sobre Ventas y Volumen
        #Me quedo unicamente con las columnas importantes
        df_ventas_y_volumen = df_ventas_y_volumen[['Año', 'Mes', 'Direccion', 'Punto Operacional', 'Sector', 'Seccion', 'Grupo de Familia', 'Ventas c/impuesto', 'Venta en Unidades']]

        #Renombro las columnas
        df_ventas_y_volumen.columns = (df_ventas_y_volumen.columns.str.strip().str.lower().str.replace(" ", "_"))
        df_ventas_y_volumen.rename(columns={
        'ventas_c/impuesto':'venta',
        'venta_en_unidades':'volumen'
        }, inplace=True)

        #Genero una columna para Obtener el ID tienda
        df_ventas_y_volumen['numero_operacional'] = df_ventas_y_volumen['punto_operacional'].str.split('-').str[0]

        #Me quedo con las columnas necesarias
        ventas = df_ventas_y_volumen[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'seccion', 'grupo_de_familia', 'venta']]
        volumen = df_ventas_y_volumen[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'sector', 'seccion', 'grupo_de_familia', 'volumen']]

        #Quito los NA de las columans de valores
        ventas.dropna(subset=['venta'], how='any', inplace=True)
        volumen.dropna(subset=['volumen'], how='any', inplace=True)

        #Realizo transformaciones para quitar carateres y convertir las columnas a valores numericos
        ventas['venta'] = ventas['venta'].str.replace('.','').str.replace(',','.').astype('float')
        volumen['volumen'] = volumen['volumen'].str.split(',').str[0].str.replace('.','').astype('int')

        #Renombro las columnas con valores de ambos DF
        ventas.rename(columns={
        'venta':'valores'
        }, inplace=True)

        volumen.rename(columns={
        'volumen':'valores'
        }, inplace=True)

        #Categorizo los valores tanto de volumne como de Ventas
        ventas['categoria'] = 'VCT'
        volumen['categoria'] = 'VOL'

        #Agrupo las ventas
        ventas_agrupado = ventas.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria'])['valores'].sum().reset_index()

        #Quito Envases del Volumen y Agrupo
        volumen_sin_vol = volumen[~volumen['grupo_de_familia'].str.contains('ENVASES')]
        volumen_agrupado = volumen_sin_vol.groupby(['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria'])['valores'].sum().reset_index()

        # Trabajo sobre Debitos
        # Renombro el DF
        debitos_agrupados = df_debitos

        # Renombro las columnas como corresponden
        debitos_agrupados.columns = debitos_agrupados.columns.str.lower().str.replace(' ','_')
        debitos_agrupados.rename(columns={
        'cant._tickets_por_local':'valores'
        }, inplace=True)

        # Renombro la columna de Debitos a valores
        debitos_agrupados['categoria'] = 'DEB'

        # Genero una columna Categorica
        debitos_agrupados['numero_operacional'] = debitos_agrupados['punto_operacional'].str.split('-').str[0]

        # Genero columna para el ID tienda
        debitos_agrupados = debitos_agrupados[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'categoria', 'valores']]

        # Quito nulos numericos de la columna valores
        debitos_agrupados.dropna(subset=['valores'], how='any', inplace=True)

        # Convierto la columna de valores a su tipo de datos correspondiente
        debitos_agrupados['valores'] = debitos_agrupados['valores'].str.replace('.','').astype(int)

        # Trabajo sobre el padron
        # Selecciono las columnas que me sirven del padron
        padron = padron[['N°', 'NOMBRE', 'Fecha apertura', 'ORGANIZACIÓN ', 'M² SALÓN', 'M² PGC', 'M² PFT', 'M² BAZAR', 'M² Electro', 'M² Textil', 'M² Pls', 'M² GALERIAS', 'PROVINCIA', 'M² Parcking', 'FIN DE CIERRE', 'ENE.2', 'FEB.2', 'MAR.2', 'ABR.2', 'MAY.2', 'JUN.2', 'JUL.2', 'AGO.2', 'SEP.2', 'OCT.2', 'NOV.2', 'DIC.2']] #type:ignore

        # Cambio de nombres en el padron
        padron.columns = (
        padron.columns
        .str.lower()
        .str.strip()
        .str.replace(' ', '_', regex=False)
        .str.replace('m²', 'm', regex=False)
        .str.replace('.2','')
        )

        # Formateo la fecha para que tenga sentido
        padron['fecha_apertura'] = padron['fecha_apertura'].dt.strftime('%d/%m/%Y')

        # Cambio el nombre de la columna N por "Numero Operacional"
        padron.rename(columns={'n°':'numero_operacional'}, inplace=True)

        # Quito los valores nulos utilizando como referencia la columna Numero Operacional, nombre y fecha apertura
        padron.dropna(subset=['numero_operacional', 'nombre', 'fecha_apertura', mes_comparable[0:3].lower()], how='any', inplace=True)

        # Genero una funcion para convertir los valores de una columna a mayuscula
        def maysc(df: pd.DataFrame, columna: str):
            df[columna] = df[columna].str.upper()

        #Aplico la formula a la columna del mes comparable para que todos los valores sean en mayuscula
        maysc(padron, mes_comparable[0:3].lower())

        # Coloco el numero operacional como numero
        padron['numero_operacional'] = padron['numero_operacional'].astype(int)

        # Concateno todos los df (venta, debito y volumen) y lo joineo con el padron
        df = pd.concat([ventas_agrupado, volumen_agrupado, debitos_agrupados])

        # Convierto el ID a numero
        df['numero_operacional'] = df['numero_operacional'].astype(int)

        # Genero el Join del df Agupado con el Padron con el objetivo de quedarme unicamente con aquellas tiendas Comparables
        df_join = df.merge(padron, how='left', on='numero_operacional')

        # Trabajo sobre Progresiones Total Formato
        # Me quedo unicamente con las columnas que me sirven del DF Joineado (ACA TENGO LA SC DEL MES)
        df_join = df_join[['año', 'mes', 'direccion', 'numero_operacional', 'punto_operacional', 'fecha_apertura', 'fin_de_cierre', 'provincia','categoria', 'valores', mes_comparable[0:3].lower()]]

        #Renombro la Columna Mes a Fecha para Luego generar la Columna Mes Correspondiente
        df_join.rename(columns={
            'mes':'fecha'
        }, inplace=True)
        df_join['mes'] = df_join['fecha'].str.split(' ').str[0]

        try:
            # Exporto todas las tablas a un archivo Excel en memoria
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_join.to_excel(writer, sheet_name=f"Data COnsolidada - SC", index=False)

            output.seek(0)
            return output

        except Exception as e:
            print(e)
            return None

    except Exception as e:
        return f'Hubo un error en el medio del flujo/pipeline. Detalle del error: {e}'
import streamlit as st
import streamlit_authenticator as stauth
import base64
import os
import copy

def convertir_a_dict(obj):
    if isinstance(obj, dict):
        return {k: convertir_a_dict(v) for k, v in obj.items()}
    elif hasattr(obj, "_asdict"):
        return convertir_a_dict(obj._asdict())
    else:
        return obj

config = convertir_a_dict(copy.deepcopy(st.secrets._secrets))

authenticator = stauth.Authenticate(
    config['credentials'], #type:ignore
    config['cookie']['name'],#type:ignore
    config['cookie']['key'],#type:ignore
    config['cookie']['expiry_days']#type:ignore
)

# Mostrar login
authenticator.login(
    location="main",
    fields={
        "Form name": "🔐 Iniciar sesión",
        "Username": "Usuario",
        "Password": "Contraseña",
        "Login": "Ingresar"
    },
    key="login"
)

authentication_status = st.session_state.get("authentication_status")
name = st.session_state.get("name")
username = st.session_state.get("username")

# Control de acceso
if authentication_status:
    
    st.sidebar.title(f'CGD Tool Box - {username}')
    st.sidebar.success(f"Bienvenido, {name} 👋")
    st.sidebar.info("Navegá entre las páginas desde la barra lateral")
    authenticator.logout("Cerrar sesión", "sidebar")

    # 🎨 Configuración inicial
    st.set_page_config(
        layout='wide',
        page_title='Carrefour Tool Box',
        page_icon='📊'
    )

    # 🎯 Encabezado
    st.markdown(
        """
        <h1 style='text-align: center; color: #0058a6;'>
            📊 Carrefour Tool Box
        </h1>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        "<h3 style='text-align: center; color: #e81c26;'>Kit de herramientas para Control de Gestión & Data Analytics</h3>",
        unsafe_allow_html=True
    )

    st.divider()

    # 📝 Intro
    st.markdown(
        """
        Bienvenido al **Carrefour Util Pack** 👋  

        En esta plataforma encontrarás un conjunto de **herramientas internas** diseñadas para simplificar y optimizar 
        el trabajo de nuestro equipo en **Control de Gestión y Data Analytics**.  
        Aquí podrás:
        - 📈 Analizar progresiones de ventas, volumen y débitos.  
        - 🗂️ Consultar padrones y superficies comparables.  
        - ⚙️ Acceder a utilidades adicionales que iremos sumando con el tiempo.  

        ---
        """
    )

    # 🔗 Secciones (modo cards en columnas)
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### 📊 Progresiones MMAA")
        st.markdown("Analiza ventas, volumen y débitos a nivel compañia por mes cerrado con comparabilidad (SC).")
        st.button("Ir a Progresiones", use_container_width=True, )

    with col2:
        st.markdown("### 🏪 Progresiones Acumuladas")
        st.markdown("Analiza ventas, volumen y débitos por Formato en base a un periodo acumulado con comparabilidad (SC).")
        st.button("Ir a Padrones", use_container_width=True)

    with col3:
        st.markdown("### 🔧 Comparacion Tiendas")
        st.markdown("Compara ventas, volumen y débitos por periodo acumulado con comparabilidad (SC) contra el formato total.")
        st.button("Explorar", use_container_width=True)


    st.markdown(
        """
        <div style='text-align: center; color: gray; font-size: 12px;'>
            Carrefour Argentina · Equipo de Control de Gestión · Data & Analytics  
        </div>
        """,
        unsafe_allow_html=True
    )

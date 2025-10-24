import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import sqlite3
from datetime import datetime, timedelta
import uuid
import re
import os 

# --- 1. CONFIGURACIÓN INICIAL Y DATOS DE PRUEBA ---

st.set_page_config(
    layout="wide", 
    page_title="Observatorio Urbano Predictivo",
    page_icon="🏙️"
)

# (## CSS GLOBAL MODERNO ##)
st.markdown("""
<style>
    :root {
        --primary-color: #008000; /* Verde Oscuro (IFTS) */
        --secondary-color: #006400; /* Verde más oscuro para hover */
        --background-color: #f4f7f4; /* Un verde-grisáceo muy claro */
        --card-background: #FFFFFF;
        --text-color: #333333;
        --border-radius: 12px;
        --box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
    }
    body { font-family: 'Segoe UI', 'Roboto', 'Helvetica Neue', 'Arial', sans-serif; }
    .stApp { background-color: var(--background-color); }
    [data-testid="stContainer"] {
        background-color: var(--card-background);
        border-radius: var(--border-radius);
        box-shadow: var(--box-shadow);
        padding: 20px;
    }
    [data-testid="stMetric"] {
        background-color: var(--card-background);
        border-radius: var(--border-radius);
        padding: 15px 20px;
        box-shadow: var(--box-shadow);
        border-left: 5px solid var(--primary-color);
    }
    [data-testid="stMetricLabel"] { font-size: 16px; color: #555; }
    [data-testid="stMetricValue"] { font-size: 32px; font-weight: 600; color: var(--primary-color); }
    .stButton>button {
        background-color: var(--primary-color);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 10px 24px;
        font-weight: 600;
        transition: background-color 0.3s ease;
    }
    .stButton>button:hover { background-color: var(--secondary-color); color: white; }
    .stButton>button[kind="secondary"] { background-color: #f0f2f6; color: var(--text-color); }
    .stButton>button[kind="secondary"]:hover { background-color: #e0e2e6; color: var(--text-color); }
    h1, h2, h3 { color: var(--primary-color); font-weight: 600; }
    [data-testid="stSidebar"] { background-color: var(--card-background); padding: 10px; }
    [data-testid="stSidebar"] .stButton>button {
        background-color: transparent;
        color: var(--text-color);
        width: 100%;
        text-align: left;
        padding-left: 15px;
        font-size: 16px;
    }
    [data-testid="stSidebar"] .stButton>button:hover {
        background-color: var(--background-color);
        color: var(--primary-color);
    }
</style>
""", unsafe_allow_html=True)


CSV_FILE_NAME = 'observatorioObrasUrbanas_limpio.csv'
DB_NAME = 'db_observatorio.sqlite'

# Inicializar estados de sesion (sin cambios)
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'username' not in st.session_state:
    st.session_state.username = None
if 'role' not in st.session_state:
    st.session_state.role = None
if 'data' not in st.session_state:
    st.session_state.data = None
if 'initial_load_success' not in st.session_state: 
    st.session_state.initial_load_success = False

# --- 2. FUNCIONES DE BASE DE DATOS (SQLite) ---
# (Sin cambios en la lógica de DB - Tu código es robusto)

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT,
            role TEXT
        )
    ''')
    users = [("admin", "admin", "admin"), ("usuario", "user", "usuario")]
    for user in users:
        try:
            cursor.execute("INSERT INTO users VALUES (?, ?, ?)", user)
        except sqlite3.IntegrityError: pass
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS proyectos (
            id TEXT PRIMARY KEY, nombre TEXT, etapa TEXT, tipo TEXT,
            monto_contrato REAL, comuna INTEGER, barrio TEXT,
            lat REAL, lng REAL, fecha_inicio TEXT, fecha_fin_inicial TEXT,
            licitacion_oferta_empresa TEXT
        )
    ''')
    conn.commit()
    conn.close()

@st.cache_data(ttl=600)
def load_initial_data_from_csv():
    conn = sqlite3.connect(DB_NAME)
    if pd.read_sql("SELECT COUNT(*) FROM proyectos", conn).iloc[0, 0] == 0:
        try:
            df = pd.read_csv(CSV_FILE_NAME, sep=',', encoding='utf-8', low_memory=False)
            df['monto_contrato'] = pd.to_numeric(df['monto_contrato'], errors='coerce').fillna(0)
            df['comuna'] = pd.to_numeric(df['comuna'], errors='coerce').fillna(0).astype(int)
            for col in ['lat', 'lng']:
                df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                df[col] = df[col].apply(lambda x: re.sub(r'[^\d.-]', '', x))
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            df_clean = df[['nombre', 'etapa', 'tipo', 'monto_contrato', 'comuna', 'barrio', 'lat', 'lng', 'fecha_inicio', 'fecha_fin_inicial', 'licitacion_oferta_empresa']].copy()
            df_clean['id'] = [str(uuid.uuid4()) for _ in range(len(df_clean))]
            df_clean.to_sql('proyectos', conn, if_exists='append', index=False)
            conn.close()
            return True
        except FileNotFoundError:
            conn.close()
            return False, "FileNotFound"
        except Exception as e:
            conn.close()
            return False, str(e)
    conn.close()
    return True 

@st.cache_data(ttl=60)
def get_all_projects_from_db():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql("SELECT * FROM proyectos", conn)
    conn.close()
    return df

def get_all_users_from_db():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql("SELECT username, role FROM users", conn)
    conn.close()
    return df

# --- 3. FUNCIONES DE AUTENTICACIÓN Y REGISTRO ---
# (Sin cambios en la lógica)

def authenticate(username, password):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT password, role FROM users WHERE username=?", (username,))
    result = cursor.fetchone()
    conn.close()
    if result and result[0] == password:
        st.session_state.authenticated = True
        st.session_state.username = username
        st.session_state.role = result[1]
        st.success(f"¡Bienvenido, {username}! Rol: {st.session_state.role.upper()}")
    else:
        st.error("Usuario o contraseña incorrectos.")
    st.rerun() 

def register_user_db(username, password):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO users (username, password, role) VALUES (?, ?, ?)", (username, password, "usuario"))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False
    except Exception as e:
        conn.close()
        st.error(f"Error interno al registrar: {e}")
        return False
    
def update_user_role_db(username, new_role):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE users SET role = ? WHERE username = ?", (new_role, username))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        conn.close()
        st.error(f"Error al actualizar el rol: {e}")
        return False

def logout():
    st.session_state.authenticated = False
    st.session_state.username = None
    st.session_state.role = None
    st.info("Sesión cerrada.")
    st.rerun()

# --- 4. FUNCIONES DE LIMPIEZA Y ANÁLISIS DE DATOS ---
# (Sin cambios en la lógica de Pandas)

@st.cache_data(show_spinner="Analizando datos y calculando métricas...", ttl=15)
def clean_and_analyze(df):
    if df.empty:
        return df, {}
    df_copy = df.copy()
    df_copy['monto_contrato'] = pd.to_numeric(df['monto_contrato'], errors='coerce').fillna(0)
    df_copy['comuna'] = pd.to_numeric(df['comuna'], errors='coerce').fillna(0).astype(int)
    for col in ['lat', 'lng']:
        df_copy[col] = df_copy[col].astype(str).str.replace(',', '.', regex=False)
        df_copy[col] = df_copy[col].apply(lambda x: re.sub(r'[^\d.-]', '', x))
        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0.0)
    df_copy['fecha_inicio'] = pd.to_datetime(df_copy['fecha_inicio'], errors='coerce')
    df_copy['fecha_fin_inicial'] = pd.to_datetime(df_copy['fecha_fin_inicial'], errors='coerce')
    diferencia_td = df_copy['fecha_fin_inicial'] - df_copy['fecha_inicio']
    diferencia_dias = diferencia_td.dt.days.fillna(0)
    df_copy['duracion_meses'] = (diferencia_dias / 30.4375).round(1)
    etapas_map = {
        'Finalizada': 'Finalizada', 'Finalizado': 'Finalizada', 'Proyecto finalizado': 'Finalizada',
        'En ejecucion': 'En Ejecución', 'En ejecución': 'En Ejecución', 'En obra': 'En Ejecución',
        'En licitacion': 'Planificada/Inactiva', 'En licitación': 'Planificada/Inactiva',
        'Adjudicada': 'Planificada/Inactiva', 'En armado de pliegos': 'Planificada/Inactiva',
        'En proyecto': 'Planificada/Inactiva',
        'Rescisión': 'No Continúa', 'Neutralizada': 'No Continúa', 'Desestimada': 'No Continúa'
    }
    df_copy['etapa_normalizada'] = df_copy['etapa'].astype(str).map(etapas_map).fillna('Otras/Sin Dato')
    df_copy['demora_dias'] = np.where(
        df_copy['etapa_normalizada'] == 'Finalizada',
        np.random.randint(-15, 60, size=len(df_copy)), 0
    )
    total_inversion = df_copy['monto_contrato'].sum()
    proyectos_activos = df_copy[df_copy['etapa_normalizada'] == 'En Ejecución'].shape[0]
    inversion_por_barrio = df_copy.groupby('barrio')['monto_contrato'].sum().nlargest(1)
    top_barrio = f"{inversion_por_barrio.index[0]} (${inversion_por_barrio.values[0]:,.0f} ARS)" if not inversion_por_barrio.empty else "N/A"
    metrics = {
        'total_inversion': total_inversion,
        'proyectos_activos': proyectos_activos,
        'top_barrio': top_barrio
    }
    if 'id' not in df_copy.columns:
        df_copy['id'] = [str(uuid.uuid4()) for _ in range(len(df_copy))]
    return df_copy, metrics

def calculate_mro_index(df):
    finalizada = df[df['etapa_normalizada'] == 'Finalizada'].groupby('barrio')['monto_contrato'].sum()
    activa = df[df['etapa_normalizada'] == 'En Ejecución'].groupby('barrio')['monto_contrato'].sum()
    mro_df = pd.DataFrame({'Activa': activa, 'Finalizada': finalizada}).fillna(0)
    mro_df['MRO Index'] = np.where(mro_df['Finalizada'] > 0, mro_df['Activa'] / mro_df['Finalizada'], np.nan)
    mro_df['Estrategia'] = np.select(
        [mro_df['MRO Index'] > 1.5, mro_df['MRO Index'] >= 0.5, mro_df['Finalizada'] > 0],
        ['Construir (Alto Crecimiento)', 'Construir / Comprar (Mixta)', 'Comprar (Estable / Madura)'],
        default='Potencial sin Datos'
    )
    return mro_df.reset_index()

def get_contratista_demora(df_finalizadas):
    if 'licitacion_oferta_empresa' not in df_finalizadas.columns:
         df_finalizadas['licitacion_oferta_empresa'] = 'SIN CONTRATISTA'
    contratista_demora_df = df_finalizadas.groupby('licitacion_oferta_empresa').agg(
        Proyectos_Finalizados=('id', 'count'), 
        Demora_Promedio=('demora_dias', 'mean'),
        Monto_Total=('monto_contrato', 'sum')
    ).reset_index()
    contratista_demora_df['Demora_Promedio'] = contratista_demora_df['Demora_Promedio'].round(0).astype(int)
    contratista_demora_df['Riesgo'] = np.where(
        contratista_demora_df['Demora_Promedio'] > 30, 
        'ALTO (Riesgo de Timing)', 'BAJO (Fiable)'
    )
    return contratista_demora_df

def generate_executive_report(df_filtered, selected_barrio, contratista_demora_df, mro_index_df):
    if df_filtered.empty:
        return "No hay datos para generar el informe."
    mro_data = mro_index_df[mro_index_df['barrio'] == selected_barrio].iloc[0] if selected_barrio in mro_index_df['barrio'].values else None
    estrategia = mro_data['Estrategia'] if mro_data is not None else 'N/A'
    mro_index = f"{mro_data['MRO Index']:.2f}" if mro_data is not None and not np.isnan(mro_data['MRO Index']) else 'N/A'
    demora_promedio = contratista_demora_df['Demora_Promedio'].mean()
    riesgo_operacional = f"{demora_promedio:.1f} días" if not np.isnan(demora_promedio) else "N/A"
    inversion_activa = df_filtered[df_filtered['etapa_normalizada'] == 'En Ejecución']['monto_contrato'].sum()
    proyectos_activos_count = df_filtered[df_filtered['etapa_normalizada'] == 'En Ejecución'].shape[0]
    report = f"""
    ### 📈 **Informe Ejecutivo de Inversión: {selected_barrio}**
    **Estrategia Recomendada:** **{estrategia}**
    - **MRO Index (Activa/Finalizada):** {mro_index} (Indica la presión de crecimiento en la zona).
    - **Inversión Activa Pendiente:** ${inversion_activa:,.0f} ARS en {proyectos_activos_count} proyectos.
    ---
    #### **Análisis de Riesgo Operacional (Contratistas)**
    - **Demora Media de Ejecución (Histórica en el Barrio):** Los contratistas que operan en esta zona tienen una demora promedio de **{riesgo_operacional}** en proyectos finalizados.
    - **Recomendación Táctica (Timing):** Si la estrategia es 'Construir', presupueste un margen de tiempo adicional de **{demora_promedio * 1.5:.0f} días** en la planificación de su salida al mercado, debido a posibles riesgos de ejecución.
    """
    return report.replace('    ', '')


# --- 5. FUNCIONES CRUD DE PROYECTOS (SQLite) ---
# (Sin cambios en la lógica)

def create_project_db(data):
    conn = sqlite3.connect(DB_NAME)
    data['fecha_inicio'] = data['fecha_inicio'].strftime('%Y-%m-%d')
    data['fecha_fin_inicial'] = data['fecha_fin_inicial'].strftime('%Y-%m-%d')
    conn.execute(
        "INSERT INTO proyectos (id, nombre, etapa, tipo, monto_contrato, comuna, barrio, lat, lng, fecha_inicio, fecha_fin_inicial, licitacion_oferta_empresa) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (str(uuid.uuid4()), data['nombre'], data['etapa'], data['tipo'], data['monto_contrato'], data['comuna'], data['barrio'], data['lat'], data['lng'], data['fecha_inicio'], data['fecha_fin_inicial'], data['licitacion_oferta_empresa'])
    )
    conn.commit()
    conn.close()
    st.cache_data.clear()
    st.toast("Proyecto creado exitosamente en SQLite.")

def delete_project_db(project_id):
    conn = sqlite3.connect(DB_NAME)
    conn.execute("DELETE FROM proyectos WHERE id=?", (project_id,))
    conn.commit()
    conn.close()
    st.cache_data.clear()
    st.toast("Proyecto eliminado de SQLite.")

# --- 6. LÓGICA DE DIBUJO Y PÁGINAS ---

# (## LOGIN MODERNO CON st.columns ##)
def draw_login_page():
    """Dibuja la página de login mobile-first, usando st.columns y CSS."""
    
    st.markdown("""
        <style>
        /* Escondemos el header y footer de Streamlit en la pág de login */
        header, footer {
            visibility: hidden !important;
        }
        /* Damos un padding superior a la página de login */
        .stApp {
            padding-top: 5vh;
        }

        /* Creamos la tarjeta de login apuntando al div de st.columns */
        div[data-testid="stHorizontalBlock"] {
            background: var(--card-background);
            border-radius: var(--border-radius);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
            max-width: 900px;
            margin: 0 auto; /* Centrar la tarjeta */
            overflow: hidden; /* Para los bordes redondeados */
        }

        /* Columna 1: Branding */
        div[data-testid="stHorizontalBlock"] > div:first-child > div {
            padding: 40px;
            background-color: #fcfcfc; /* Fondo ligero para la columna de branding */
            border-right: 1px solid #eee;
            height: 100%; /* Asegura misma altura en desktop */
            display: flex;
            flex-direction: column;
            justify-content: center; /* Centrado vertical */
        }
        
        /* Columna 2: Formulario */
        div[data-testid="stHorizontalBlock"] > div:last-child > div {
            padding: 30px 40px;
            height: 100%;
        }

        /* Mobile-first: Streamlit apila las columnas automáticamente */
        @media (max-width: 768px) {
            div[data-testid="stHorizontalBlock"] {
                max-width: 500px; /* Ancho más amigable en móvil */
            }
            div[data-testid="stHorizontalBlock"] > div:first-child > div {
                border-right: none;
                border-bottom: 1px solid #eee;
            }
        }
        </style>
    """, unsafe_allow_html=True)

    with st.container():
        col1, col2 = st.columns([1, 1], gap="small") # Dos columnas [Branding, Form]

        with col1:
            st.markdown("<h2>🏙️ Observatorio Urbano</h2>", unsafe_allow_html=True)
            st.markdown("""
            <p style="font-size: 16px; color: #555; line-height: 1.5;">
            Bienvenido al dashboard de análisis predictivo de inversiones 
            inmobiliarias. Ingrese sus credenciales para acceder.
            </p>
            """, unsafe_allow_html=True)

        with col2:
            tab_login, tab_register = st.tabs(["Iniciar Sesión", "Registrarse"])

            with tab_login:
                with st.form("login_form"):
                    username = st.text_input("Usuario", key='login_user')
                    password = st.text_input("Contraseña", type="password", key='login_pass')
                    submitted = st.form_submit_button("Ingresar", type="primary", use_container_width=True)
                    
                    if submitted:
                        authenticate(username, password)
                
                st.markdown("---")
                st.write("Acceso Rápido:")
                st.caption("Admin: `admin / admin` | Usuario: `usuario / user`")

            with tab_register:
                with st.form("register_form"):
                    new_user = st.text_input("Nuevo Usuario", key='reg_user_2')
                    new_pass = st.text_input("Nueva Contraseña", type="password", key='reg_pass_2')
                    submitted_reg = st.form_submit_button("Registrarse", type="secondary", use_container_width=True)
                    
                    if submitted_reg:
                        if not new_user or not new_pass:
                            st.error("Por favor, ingrese un usuario y contraseña válidos.")
                        elif register_user_db(new_user, new_pass):
                            st.success("Usuario registrado exitosamente. Por favor, inicie sesión.")
                        else:
                            st.error("Error: El nombre de usuario ya existe.")

def draw_sidebar():
    """Dibuja la barra lateral de navegación."""
    with st.sidebar:
        st.title(f"🏙️ {st.session_state.username.upper()}")
        st.caption(f"Rol: {st.session_state.role.upper()}")
        st.markdown("---")
        st.header("Análisis Estratégico")
        
        if st.button("Dashboard de Análisis", key="nav_dashboard", use_container_width=True):
            st.session_state.page = "dashboard"
            st.rerun() 
        if st.button("Riesgo Operacional", key="nav_riesgo", use_container_width=True):
            st.session_state.page = "riesgo"
            st.rerun() 
        if st.button("Administración (CRUD)", key="nav_crud", disabled=(st.session_state.role != 'admin'), use_container_width=True):
            st.session_state.page = "crud"
            st.rerun() 
        
        st.markdown("---")
        st.button("Cerrar Sesión", on_click=logout, type="secondary", use_container_width=True)

def draw_dashboard_content(df_analyzed, metrics, mro_index_df, demora_contratista_df):
    """Dibuja el contenido del Dashboard con el filtro del mapa corregido."""
    
    st.title("🏙️ Observatorio Inmobiliario Urbano")
    st.header("Dashboard de Oportunidades (Estrategia Predictiva)")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Inversión Total", f"${metrics['total_inversion']:,.0f}", help="Monto total contratado en el dataset.")
    with col2:
        st.metric("Proyectos Activos", f"{metrics['proyectos_activos']} Proyectos", help="Obras en etapa 'En Ejecución'.")
    with col3:
        st.metric("Top Barrio", metrics['top_barrio'].split(' ')[0], help="Barrio con mayor inversión total.")
    with col4:
        mro_avg = mro_index_df['MRO Index'].mean() if not mro_index_df['MRO Index'].empty else 0
        st.metric("Índice MRO Promedio", f"{mro_avg:.2f}", help="Ratio Inversión Activa/Finalizada. > 1 = Alto Crecimiento.")
    
    st.markdown("---")
    
    col_map_real, col_trend = st.columns([2, 1])

    with col_map_real:
        st.subheader("Mapa de Intensidad de Proyectos")
        
        # Corrección del Mapa: Filtro de Bounding Box para CABA
        df_map = df_analyzed[
            (df_analyzed['lat'] > -34.71) & (df_analyzed['lat'] < -34.53) &
            (df_analyzed['lng'] > -58.54) & (df_analyzed['lng'] < -58.33)
        ].copy()
        
        if df_map.empty:
            st.warning("""
            **No se pudieron cargar los datos del mapa.** No se encontraron proyectos con coordenadas (lat/lng) válidas dentro 
            del rango de CABA.
            """)
        else:
            df_map['size_map'] = np.log(df_map['monto_contrato'] + 1)
            st.map(
                df_map,
                latitude='lat',
                longitude='lng',
                size='size_map',
                color='#00800060', # Verde con 60% de transparencia
                zoom=11
            )
            st.caption("El tamaño de los círculos es proporcional al logaritmo del monto del contrato.")

    with col_trend:
        with st.container(): 
            st.subheader("Evolución (Tendencia)")
            df_analyzed['anio_inicio'] = df_analyzed['fecha_inicio'].dt.year
            df_trend = df_analyzed.groupby('anio_inicio')['monto_contrato'].sum().reset_index()
            df_trend = df_trend[df_trend['anio_inicio'].notna()].sort_values(by='anio_inicio')
            
            fig_trend = px.area(df_trend, x='anio_inicio', y='monto_contrato', 
                                title='Inversión Contratada por Año',
                                labels={'monto_contrato': 'Monto (ARS)', 'anio_inicio': 'Año'},
                                markers=True)
            fig_trend.update_traces(line=dict(color=st.get_option("theme.primaryColor")), fillcolor='rgba(0,128,0,0.2)')
            fig_trend.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_trend, use_container_width=True)

    st.markdown("---")
    
    col_vis_1, col_vis_2 = st.columns([1, 1])
    with col_vis_1:
        with st.container():
            st.subheader("Prioridad de Inversión (Tipología)")
            df_inversion_tipo = df_analyzed.groupby('tipo')['monto_contrato'].sum().reset_index()
            df_inversion_tipo = df_inversion_tipo.sort_values(by='monto_contrato', ascending=False).head(10)
            fig_inversion = px.bar(df_inversion_tipo, x='monto_contrato', y='tipo', orientation='h', 
                                   labels={'monto_contrato': 'Monto (ARS)', 'tipo': 'Tipo de Proyecto'}, 
                                   color='monto_contrato', color_continuous_scale=px.colors.sequential.Greens_r)
            fig_inversion.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_inversion, use_container_width=True)
    with col_vis_2:
        with st.container():
            st.subheader("Distribución por Comuna")
            df_treemap = df_analyzed.groupby(['comuna', 'tipo'])['monto_contrato'].sum().reset_index()
            df_treemap['comuna_str'] = 'Comuna ' + df_treemap['comuna'].astype(str)
            fig_treemap = px.treemap(df_treemap, path=[px.Constant("CABA"), 'comuna_str', 'tipo'], 
                                     values='monto_contrato', color='monto_contrato', 
                                     color_continuous_scale='Greens', title="Concentración por Comuna y Tipo")
            fig_treemap.update_layout(paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_treemap, use_container_width=True)

    st.markdown("---")
    
    with st.container():
        st.subheader("Proyectos en Ejecución (Ventana de Oportunidad)")
        df_ejecucion = df_analyzed[df_analyzed['etapa_normalizada'] == 'En Ejecución']
        df_ejecucion_grouped = df_ejecucion.groupby('barrio').agg(
            Proyectos=('nombre', 'count'),
            Inversion_Activa=('monto_contrato', 'sum'),
            Duracion_Promedio=('duracion_meses', 'mean')
        ).reset_index()
        df_ejecucion_grouped['Duracion_Promedio'] = df_ejecucion_grouped['Duracion_Promedio'].round(1)
        st.dataframe(df_ejecucion_grouped.sort_values(by='Inversion_Activa', ascending=False), 
                     hide_index=True, use_container_width=True,
                     column_config={
                         "barrio": "Barrio", 
                         "Inversion_Activa": st.column_config.NumberColumn("Inversión Activa (ARS)", format="$ %i"), 
                         "Duracion_Promedio": st.column_config.NumberColumn("Duración Promedio (Meses)", format="%.1f meses"), 
                         "Proyectos": st.column_config.NumberColumn("Conteo")
                     })

def draw_riesgo_page(df_analyzed, metrics, mro_index_df, demora_contratista_df):
    """Dibuja el contenido de Riesgo Operacional."""
    st.title("🏙️ Observatorio Inmobiliario Urbano")
    st.header("Análisis de Riesgo Operacional (Modelo de Contratistas)")
    with st.container():
        st.subheader("Demora Histórica por Contratista (Riesgo de Ejecución)")
        st.dataframe(demora_contratista_df.sort_values(by='Demora_Promedio', ascending=False), 
                     hide_index=True, use_container_width=True,
                     column_config={
                         "licitacion_oferta_empresa": "Contratista",
                         "Proyectos_Finalizados": "Proyectos Finalizados",
                         "Demora_Promedio": st.column_config.NumberColumn("Demora Promedio (Días)", format="%d días"),
                         "Monto_Total": st.column_config.NumberColumn("Monto Invertido (ARS)", format="$ %,.0f"),
                         "Riesgo": "Nivel de Riesgo Operacional"
                     })
        st.markdown("**Insight:** Los contratistas con alta demora histórica (Riesgo ALTO) aumentan el riesgo de 'timing' en su inversión de construcción.")
    st.markdown("---")
    with st.container():
        st.subheader("Generador de Informe Ejecutivo (Simulador de Decisión)")
        valid_barrios = sorted(df_analyzed['barrio'].dropna().unique().tolist())
        selected_barrio = st.selectbox("Seleccione el Barrio para el Informe:", options=valid_barrios)
        contratistas_en_barrio = df_analyzed[df_analyzed['barrio'] == selected_barrio]['licitacion_oferta_empresa'].unique()
        df_demora_filtrada = demora_contratista_df[demora_contratista_df['licitacion_oferta_empresa'].isin(contratistas_en_barrio)]
        if st.button("Generar Informe Predictivo", type="primary"):
            report = generate_executive_report(df_analyzed[df_analyzed['barrio'] == selected_barrio], selected_barrio, df_demora_filtrada, mro_index_df)
            st.markdown(report)
            st.success("Informe generado con éxito.")

def draw_crud_page(df_analyzed):
    """Dibuja la página de Administración."""
    st.title("🏙️ Observatorio Inmobiliario Urbano")
    st.header("Administración (CRUD) y Gestión de Usuarios")
    st.markdown("---")
    col_crud_form, col_user_management = st.columns(2)
    with col_crud_form:
        with st.container():
            with st.form("project_form_create"):
                st.markdown("##### Nuevo Proyecto")
                nombre = st.text_input("Nombre del Proyecto")
                col_form_1, col_form_2 = st.columns(2)
                comuna_options = sorted(df_analyzed['comuna'].dropna().unique().tolist())
                comuna = col_form_1.selectbox("Comuna", options=comuna_options)
                barrio = col_form_2.text_input("Barrio")
                tipo_options = sorted(df_analyzed['tipo'].dropna().unique().tolist())
                tipo = st.selectbox("Tipo de Proyecto", options=tipo_options)
                monto_contrato = st.number_input("Monto Contratado (ARS)", min_value=0.0, format="%f")
                etapa_original_options = df_analyzed['etapa'].dropna().unique().tolist()
                etapa = st.selectbox("Etapa (Texto Original)", options=etapa_original_options)
                col_form_3, col_form_4 = st.columns(2)
                lat = col_form_3.number_input("Latitud", format="%f", value=-34.6037)
                lng = col_form_4.number_input("Longitud", format="%f", value=-58.3816)
                submitted = st.form_submit_button("Crear Proyecto", type="primary")
                if submitted:
                    new_data = {
                        'nombre': nombre, 'comuna': comuna, 'barrio': barrio, 'tipo': tipo,
                        'monto_contrato': monto_contrato, 'etapa': etapa, 'lat': lat, 'lng': lng,
                        'fecha_inicio': datetime.now(), 'fecha_fin_inicial': datetime.now() + timedelta(days=365 * 1.5),
                        'licitacion_oferta_empresa': st.session_state.username.upper(),
                    }
                    create_project_db(new_data)
                    st.rerun() 
        with st.container():
            st.markdown("##### Eliminar Proyecto")
            df_crud_display = df_analyzed.copy()
            df_crud_display = df_crud_display[['id', 'nombre', 'barrio', 'etapa_normalizada', 'monto_contrato']]
            selected_id_delete = st.selectbox("Seleccionar ID de Proyecto para Eliminar", 
                                                options=df_analyzed['id'].tolist(), 
                                                format_func=lambda x: f"{df_analyzed[df_analyzed['id'] == x]['nombre'].values[0]} (ID: ...{x[-6:]})",
                                                index=None, key='delete_select')
            if st.button("Confirmar Eliminación", type="secondary"):
                if selected_id_delete:
                    delete_project_db(selected_id_delete)
                    st.rerun() 
    with col_user_management:
        with st.container():
            st.subheader("Gestión de Usuarios y Roles")
            df_users = get_all_users_from_db()
            st.markdown("##### Lista de Usuarios Registrados")
            st.dataframe(df_users, hide_index=True, use_container_width=True)
            st.markdown("##### Cambiar Rol de Usuario")
            user_list = df_users['username'].tolist()
            user_to_modify = st.selectbox("Seleccionar Usuario", options=user_list, index=None, key='user_modify')
            new_role = st.selectbox("Nuevo Rol", options=['admin', 'usuario'], key='new_role')
            if st.button("Actualizar Rol", type="primary"):
                if user_to_modify and new_role:
                    if update_user_role_db(user_to_modify, new_role):
                        st.success(f"Rol de {user_to_modify} actualizado a {new_role}.")
                        st.rerun() 
                    else:
                        st.error(f"Error al actualizar el rol de {user_to_modify}.")

# --- 7. LÓGICA PRINCIPAL DE RENDERIZADO (El Controlador) ---

# (## CORRECCIÓN: LÓGICA DE INICIO Y RENDERIZADO LIMPIA ##)

def main():
    # 1. Ejecutar inicialización de DB
    init_db()
    initial_load_result = load_initial_data_from_csv()
    if initial_load_result is True:
        st.session_state.initial_load_success = True
    elif initial_load_result[0] is False:
        st.error(f"FALLA CRÍTICA DE CARGA: {initial_load_result[1]}. La aplicación no puede funcionar sin datos.")
        return # Detener la app si la carga inicial falla

    # 2. Inicializar la página actual si no está definida
    if 'page' not in st.session_state:
        st.session_state.page = "dashboard"

    # 3. Mostrar página de Login si no está autenticado
    if not st.session_state.authenticated:
        draw_login_page()
        
    # 4. Mostrar la aplicación si está autenticado
    else:
        # Cargar y analizar los datos actuales
        df_raw = get_all_projects_from_db()
        if df_raw.empty:
            st.error("No se pudieron cargar los datos de los proyectos desde la base de datos.")
            return

        df_analyzed, metrics = clean_and_analyze(df_raw)
        df_finalizadas_only = df_analyzed[df_analyzed['etapa_normalizada'] == 'Finalizada']
        demora_contratista_df = get_contratista_demora(df_finalizadas_only)
        mro_index_df = calculate_mro_index(df_analyzed)

        # Dibujar la barra lateral de navegación
        draw_sidebar()

        # Determinar qué contenido dibujar basado en el estado de la sesión
        if st.session_state.page == "dashboard":
            draw_dashboard_content(df_analyzed, metrics, mro_index_df, demora_contratista_df)
        elif st.session_state.page == "riesgo":
            draw_riesgo_page(df_analyzed, metrics, mro_index_df, demora_contratista_df)
        elif st.session_state.page == "crud" and st.session_state.role == 'admin':
            draw_crud_page(df_analyzed)
        else:
            # Fallback
            st.session_state.page = "dashboard"
            st.rerun()

if __name__ == "__main__":
    main()

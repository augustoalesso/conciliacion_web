import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import timedelta

# ==========================================================
# 🔒 LÓGICA DE ACCESO PRIVADO
# ==========================================================
try:
    VALID_USERNAME = st.secrets["users"]["encargado"]
    VALID_PASSWORD = st.secrets["users"]["AugustoBot1"]
except KeyError:
    try:
        VALID_USERNAME = st.secrets["db_credentials"]["username"]
        VALID_PASSWORD = st.secrets["db_credentials"]["password"]
    except KeyError:
        VALID_USERNAME = ""
        VALID_PASSWORD = ""

def logout():
    st.session_state["password_correct"] = False
    st.rerun()

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    if not VALID_USERNAME or not VALID_PASSWORD:
        st.error("❌ Error de Configuración: Credenciales no encontradas.")
        st.stop()
    if st.session_state["password_correct"]:
        return True
    
    st.title("🔐 Acceso Restringido")
    st.markdown("---")
    with st.form(key="login_form"):
        username = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")
        login_button = st.form_submit_button("Ingresar")
    if login_button:
        if (username == VALID_USERNAME and password == VALID_PASSWORD):
            st.session_state["password_correct"] = True
            st.rerun()
        else:
            st.error("❌ Usuario o Contraseña incorrecta.")
    return False

# ==========================================================
# --- CONFIGURACIÓN ESTÁTICA Y MAPPING ---
# ==========================================================
TOLERANCIA_DIAS = 3 
ID_COL = 'Numero Operacion ID' 

COLUMNAS_MAPEO = {
    'Fecha': 'Fecha', 'Debe': 'Debe', 'Haber': 'Haber',
    'Monto': 'Monto', 'Concepto': 'Concepto', 'Numero de operación': ID_COL 
}

def get_columnas_finales():
    return ['Estado', 'Fecha', 'Monto_C', 'Monto_B', 'Concepto_C', 'Concepto_B', f'{ID_COL}_C', f'{ID_COL}_B']

# --- Funciones de Formateo ---
def formatear_reporte_id(df_merge_id):
    COLUMNAS_RENOMBRAR = {f'Monto_C_ID': 'Monto_C', f'Monto_B_ID': 'Monto_B',
        f'Concepto_C_ID': 'Concepto_C', f'Concepto_B_ID': 'Concepto_B',
        f'{ID_COL}_C_ID': f'{ID_COL}_C', f'{ID_COL}_B_ID': f'{ID_COL}_B'}
    df_reporte = df_merge_id.rename(columns=COLUMNAS_RENOMBRAR)
    df_reporte['Estado'] = 'Conciliado por ID'
    df_reporte['Fecha'] = df_reporte['Fecha_B_ID'].fillna(df_reporte['Fecha_C_ID']) 
    return df_reporte.reindex(columns=get_columnas_finales())

def formatear_reporte_fecha(df_merge_fecha):
    df_conciliado = df_merge_fecha[df_merge_fecha['_merge'] == 'both'].copy()
    df_conciliado['Estado'] = 'Conciliado por Fecha'
    COLUMNAS_RENOMBRAR = {f'Monto_C': 'Monto_C', f'Monto_B': 'Monto_B', 
        f'Concepto_C': 'Concepto_C', f'Concepto_B': 'Concepto_B',
        f'{ID_COL}_C': f'{ID_COL}_C', f'{ID_COL}_B': f'{ID_COL}_B'}
    df_conciliado = df_conciliado.rename(columns=COLUMNAS_RENOMBRAR)
    return df_conciliado.reindex(columns=get_columnas_finales())

def formatear_reporte_pendientes(df_final_pendientes):
    df_trabajo = df_final_pendientes.copy()
    df_trabajo['Estado'] = np.select(
        [df_trabajo['_merge'] == 'left_only', df_trabajo['_merge'] == 'right_only'],
        ['Pendiente - Solo en Contabilidad', 'Pendiente - Solo en Banco'], default='Error'
    )
    df_reporte = df_trabajo[df_trabajo['Estado'] != 'Error'].copy()
    COLUMNAS_RENOMBRAR = {f'Monto_C': 'Monto_C', f'Monto_B': 'Monto_B', 
        f'Concepto_C': 'Concepto_C', f'Concepto_B': 'Concepto_B',
        f'{ID_COL}_C': f'{ID_COL}_C', f'{ID_COL}_B': f'{ID_COL}_B'}
    df_reporte = df_reporte.rename(columns=COLUMNAS_RENOMBRAR)
    df_reporte['Fecha'] = df_reporte['Fecha_C'].fillna(df_reporte['Fecha_B'])
    return df_reporte.reindex(columns=get_columnas_finales())

@st.cache_data
def cargar_datos(uploaded_file, origen):
    try:
        df = pd.read_excel(uploaded_file) 
    except Exception as e:
        st.error(f"Error al cargar el archivo de {origen}: {e}")
        return None
    df = df.rename(columns={excel_name: internal_name for excel_name, internal_name in COLUMNAS_MAPEO.items() if excel_name in df.columns})
    if origen == 'Contable' and 'Debe' in df.columns and 'Haber' in df.columns:
        df['Debe'] = pd.to_numeric(df['Debe'], errors='coerce').fillna(0)
        df['Haber'] = pd.to_numeric(df['Haber'], errors='coerce').fillna(0)
        df['Monto'] = df['Debe'] - df['Haber'] 
    elif 'Monto' in df.columns:
        df['Monto'] = pd.to_numeric(df['Monto'], errors='coerce').fillna(0) 
    df['Abs_Monto'] = df['Monto'].abs() 
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce', dayfirst=True) 
    df[ID_COL] = df[ID_COL].astype(str)
    df['ID_Original'] = df.index
    return df[['Fecha', 'Monto', 'Abs_Monto', 'Concepto', ID_COL, 'ID_Original']]

@st.cache_data
def conciliar(df_contable, df_bancario):
    columnas_finales = get_columnas_finales()
    df_c = df_contable.copy()
    df_b = df_bancario.copy()
    df_c['Conciliado'] = False
    df_b['Conciliado'] = False

    # PASO 1: ID
    df_merge_id = pd.merge(df_c, df_b, on=[ID_COL, 'Abs_Monto'], how='inner', suffixes=('_C_ID', '_B_ID'))
    if not df_merge_id.empty:
        for ic, ib in zip(df_merge_id['ID_Original_C_ID'], df_merge_id['ID_Original_B_ID']):
            df_c.loc[df_c['ID_Original'] == ic, 'Conciliado'] = True
            df_b.loc[df_b['ID_Original'] == ib, 'Conciliado'] = True
        df_reporte_id = formatear_reporte_id(df_merge_id)
    else: df_reporte_id = pd.DataFrame(columns=columnas_finales)

    # PASO 2: FECHA
    df_c_p = df_c[df_c['Conciliado'] == False].copy().reset_index(drop=True)
    df_b_p = df_b[df_b['Conciliado'] == False].copy().reset_index(drop=True)
    df_reporte_fecha = pd.DataFrame(columns=columnas_finales)
    if not df_c_p.empty and not df_b_p.empty:
        df_merge_fecha = pd.merge(df_c_p, df_b_p, on=['Fecha', 'Abs_Monto'], how='inner', suffixes=('_C', '_B'), indicator=True)
        if not df_merge_fecha.empty:
            df_reporte_fecha = formatear_reporte_fecha(df_merge_fecha)
            for ic in df_merge_fecha['ID_Original_C']: df_c.loc[df_c['ID_Original'] == ic, 'Conciliado'] = True
            for ib in df_merge_fecha['ID_Original_B']: df_b.loc[df_b['ID_Original'] == ib, 'Conciliado'] = True

    # PASO 3: TOLERANCIA
    df_c_p2 = df_c[df_c['Conciliado'] == False].copy().reset_index(drop=True)
    df_b_p2 = df_b[df_b['Conciliado'] == False].copy().reset_index(drop=True)
    tol_list = []
    for _, rc in df_c_p2.iterrows():
        fechas = [rc['Fecha'] + timedelta(days=d) for d in range(-TOLERANCIA_DIAS, TOLERANCIA_DIAS+1)]
        match = df_b_p2[(df_b_p2['Abs_Monto'] == rc['Abs_Monto']) & (df_b_p2['Fecha'].isin(fechas)) & (df_b_p2['Conciliado'] == False)]
        if not match.empty:
            rb = match.iloc[0]
            df_c.loc[df_c['ID_Original'] == rc['ID_Original'], 'Conciliado'] = True
            df_b.loc[df_b['ID_Original'] == rb['ID_Original'], 'Conciliado'] = True
            tol_list.append({'Estado': f'Conciliado (+/- {TOLERANCIA_DIAS} Días)', 'Fecha': rb['Fecha'], 'Monto_C': rc['Monto'], 'Monto_B': rb['Monto'], 'Concepto_C': rc['Concepto'], 'Concepto_B': rb['Concepto'], f'{ID_COL}_C': rc[ID_COL], f'{ID_COL}_B': rb[ID_COL]})
    df_reporte_tolerancia = pd.DataFrame(tol_list, columns=columnas_finales)

    # PENDIENTES
    df_c_f, df_b_f = df_c[df_c['Conciliado'] == False], df_b[df_b['Conciliado'] == False]
    df_m_p = pd.merge(df_c_f, df_b_f, on='Abs_Monto', how='outer', suffixes=('_C', '_B'), indicator=True)
    df_reporte_pendientes = formatear_reporte_pendientes(df_m_p)
    
    return pd.concat([df_reporte_id, df_reporte_fecha, df_reporte_tolerancia, df_reporte_pendientes]).sort_values('Fecha')

# ==========================================================
# --- GENERACIÓN DE EXCEL CON FORMATO REFORZADO ---
# ==========================================================
@st.cache_data
def to_excel_with_summary(df, cliente_nombre=""):
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='dd/mm/yyyy')
    workbook = writer.book

    # Formatos
    f_tit = workbook.add_format({'bold': True, 'font_size': 24, 'font_color': '#1E1B4B'})
    f_sub = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#4F46E5'})
    f_head = workbook.add_format({'bold': True, 'bg_color': '#F3F4F6', 'border': 1, 'align': 'center'})
    f_num = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
    f_bord = workbook.add_format({'border': 1})
    
    # Semáforo FinMatch
    color_exacto = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'border': 1}) # Verde Oscuro
    color_margen = workbook.add_format({'bg_color': '#DDEBF7', 'font_color': '#000000', 'border': 1}) # Azul Claro (Margen)
    color_contab = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500', 'border': 1}) # Amarillo
    color_banco = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'border': 1})  # Rojo
    color_posible = workbook.add_format({'bg_color': '#E0BBE4', 'font_color': '#5D0970', 'border': 1}) # Violeta

    # --- HOJA 0: CARÁTULA ---
    ws0 = workbook.add_worksheet('Certificado FinMatch')
    ws0.hide_gridlines(2)
    ws0.write('B2', 'FINMATCH', f_tit)
    ws0.write('B3', 'ConciliadorWeb - Reporte de Auditoría', f_sub)
    ws0.write('B5', 'CLIENTE / EMPRESA:', workbook.add_format({'bold': True}))
    ws0.write('C5', cliente_nombre.upper() if cliente_nombre else "________________________")
    
    ws0.write('B7', 'RESUMEN EJECUTIVO', workbook.add_format({'bold': True, 'underline': True}))
    res_data = df['Estado'].value_counts().reset_index()
    res_data.columns = ['Estado', 'Total']
    ws0.write_row('B8', res_data.columns, f_head)
    for i, row in res_data.iterrows():
        ws0.write_row(8+i, 1, row, f_bord)

    # --- HOJA 1: DETALLE ---
    ws1 = workbook.add_worksheet('Reporte Conciliación')
    df.to_excel(writer, sheet_name='Reporte Conciliación', index=False)
    rango_det = f'A2:H{len(df)+1}'
    
    # Formato condicional estricto
    ws1.conditional_format(rango_det, {'type': 'text', 'criteria': 'containing', 'value': 'por ID', 'format': color_exacto})
    ws1.conditional_format(rango_det, {'type': 'text', 'criteria': 'containing', 'value': 'por Fecha', 'format': color_exacto})
    ws1.conditional_format(rango_det, {'type': 'text', 'criteria': 'containing', 'value': 'Días', 'format': color_margen})
    ws1.conditional_format(rango_det, {'type': 'text', 'criteria': 'containing', 'value': 'Contabilidad', 'format': color_contab})
    ws1.conditional_format(rango_det, {'type': 'text', 'criteria': 'containing', 'value': 'Banco', 'format': color_banco})
    
    ws1.set_column('C:D', 15, f_num); ws1.set_column('A:A', 35); ws1.set_column('E:H', 25)

    # --- HOJA 2: RESUMEN CONCEPTOS ---
    df_p = df[df['Estado'].str.contains('Pendiente')].copy()
    if not df_p.empty:
        ws2 = workbook.add_worksheet('Resumen Conceptos')
        df_p['Conc_F'] = df_p['Concepto_C'].fillna(df_p['Concepto_B'])
        
        # Lógica de posibles coincidencias por monto
        ag = df_p.groupby(['Estado', 'Conc_F'])[['Monto_C', 'Monto_B']].sum().reset_index()
        ag['Total'] = ag['Monto_C'].fillna(0) + ag['Monto_B'].fillna(0)
        ag['Abs_Total'] = ag['Total'].abs()
        
        # Identificar duplicados de montos absolutos entre estados
        duplicados = ag[ag.duplicated('Abs_Total', keep=False)]['Abs_Total'].unique()
        ag['Control'] = np.where(ag['Abs_Total'].isin(duplicados), 'Posible Coincidencia', '')
        
        # Exportar
        ag[['Estado', 'Conc_F', 'Total', 'Control']].to_excel(writer, sheet_name='Resumen Conceptos', index=False)
        
        rango_concept = f'A2:D{len(ag)+1}'
        ws2.conditional_format(rango_concept, {'type': 'text', 'criteria': 'containing', 'value': 'Contabilidad', 'format': color_contab})
        ws2.conditional_format(rango_concept, {'type': 'text', 'criteria': 'containing', 'value': 'Banco', 'format': color_banco})
        ws2.conditional_format(rango_concept, {'type': 'text', 'criteria': 'containing', 'value': 'Posible', 'format': color_posible})
        
        ws2.set_column('A:A', 35); ws2.set_column('B:B', 45); ws2.set_column('C:D', 18, f_num)

    writer.close()
    return output.getvalue()

# ==========================================================
# --- FRONTEND ---
# ==========================================================
st.set_page_config(page_title="ConciliadorWeb by FinMatch", layout="centered")

if check_password():
    with st.sidebar:
        st.button("🚪 Cerrar Sesión", on_click=logout)
    
    st.title("Sistema ConciliadorWeb 🏦")
    st.info("Desarrollado por FinMatch para la gestión contable eficiente.")
    cliente = st.text_input("Nombre del Cliente / Empresa", placeholder="Ej: Distribuidora S.A.")

    col1, col2 = st.columns(2)
    with col1: up_c = st.file_uploader("Contabilidad", type=['xlsx'])
    with col2: up_b = st.file_uploader("Banco", type=['xlsx'])

    if st.button("▶️ EJECUTAR CONCILIACIÓN", type="primary", use_container_width=True):
        if up_c and up_b:
            with st.spinner("Procesando datos con lógica FinMatch..."):
                dc, db = cargar_datos(up_c, 'Contable'), cargar_datos(up_b, 'Banco')
                if dc is not None and db is not None:
                    resultado = conciliar(dc, db)
                    st.success("✅ Conciliación completada.")
                    st.download_button(
                        "⬇️ Descargar FinMatch_Reporte.xlsx", 
                        to_excel_with_summary(resultado, cliente), 
                        "FinMatch_Reporte.xlsx"
                    )

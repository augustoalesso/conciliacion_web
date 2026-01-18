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
except Exception:
    VALID_USERNAME = ""
    VALID_PASSWORD = ""

def logout():
    st.session_state["password_correct"] = False
    st.rerun()

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    if st.session_state["password_correct"]:
        return True

    if not VALID_USERNAME or not VALID_PASSWORD:
        st.error("❌ Error de Configuración: No se encontraron las credenciales en st.secrets.")
        st.stop()

    st.title("🔐 Acceso Restringido - FinMatch")
    with st.form(key="login_form"):
        username = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")
        if st.form_submit_button("Ingresar"):
            if username == VALID_USERNAME and password == VALID_PASSWORD:
                st.session_state["password_correct"] = True
                st.rerun()
            else:
                st.error("❌ Credenciales incorrectas.")
    return False

# ==========================================================
# --- CONFIGURACIÓN Y MAPPING ---
# ==========================================================
TOLERANCIA_DIAS = 3 
ID_COL = 'Numero Operacion ID' 

COLUMNAS_MAPEO = {
    'Fecha': 'Fecha', 'Debe': 'Debe', 'Haber': 'Haber',
    'Monto': 'Monto', 'Concepto': 'Concepto', 'Numero de operación': ID_COL
}

# --- Funciones de Formateo ---
def get_columnas_finales():
    return ['Estado', 'Fecha', 'Monto_C', 'Monto_B', 'Concepto_C', 'Concepto_B', f'{ID_COL}_C', f'{ID_COL}_B']

def formatear_reporte_id(df_merge_id):
    df_reporte = df_merge_id.rename(columns={
        'Monto_C_ID': 'Monto_C', 'Monto_B_ID': 'Monto_B',
        'Concepto_C_ID': 'Concepto_C', 'Concepto_B_ID': 'Concepto_B',
        f'{ID_COL}_C_ID': f'{ID_COL}_C', f'{ID_COL}_B_ID': f'{ID_COL}_B'
    })
    df_reporte['Estado'] = 'Conciliado por ID'
    df_reporte['Fecha'] = df_reporte['Fecha_B_ID'].fillna(df_reporte['Fecha_C_ID']) 
    return df_reporte.reindex(columns=get_columnas_finales())

def formatear_reporte_fecha(df_merge_fecha):
    df_conciliado = df_merge_fecha[df_merge_fecha['_merge'] == 'both'].copy()
    df_conciliado['Estado'] = 'Conciliado por Fecha'
    return df_conciliado.reindex(columns=get_columnas_finales())

def formatear_reporte_pendientes(df_final_pendientes):
    df_trabajo = df_final_pendientes.copy()
    df_trabajo['Estado'] = np.select(
        [df_trabajo['_merge'] == 'left_only', df_trabajo['_merge'] == 'right_only'],
        ['Pendiente - Solo en Contabilidad', 'Pendiente - Solo en Banco'],
        default='Error'
    )
    df_reporte = df_trabajo[df_trabajo['Estado'] != 'Error'].copy()
    df_reporte['Fecha'] = df_reporte['Fecha_C'].fillna(df_reporte['Fecha_B'])
    return df_reporte.reindex(columns=get_columnas_finales())

@st.cache_data
def cargar_datos(uploaded_file, origen):
    try:
        df = pd.read_excel(uploaded_file) 
    except Exception as e:
        st.error(f"Error en {origen}: {e}")
        return None
    
    df = df.rename(columns={excel_name: internal_name for excel_name, internal_name in COLUMNAS_MAPEO.items() if excel_name in df.columns})
    
    if origen == 'Contable' and 'Debe' in df.columns and 'Haber' in df.columns:
        df['Monto'] = pd.to_numeric(df['Debe'], errors='coerce').fillna(0) - pd.to_numeric(df['Haber'], errors='coerce').fillna(0)
    elif 'Monto' in df.columns:
        df['Monto'] = pd.to_numeric(df['Monto'], errors='coerce').fillna(0)
    
    df['Abs_Monto'] = df['Monto'].abs() 
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce', dayfirst=True) 
    df[ID_COL] = df[ID_COL].astype(str)
    df['ID_Original'] = df.index
    return df[['Fecha', 'Monto', 'Abs_Monto', 'Concepto', ID_COL, 'ID_Original']]

@st.cache_data
def conciliar(df_c, df_b):
    df_c['Conciliado'] = False
    df_b['Conciliado'] = False

    # PASO 1: ID
    df_merge_id = pd.merge(df_c, df_b, on=[ID_COL, 'Abs_Monto'], how='inner', suffixes=('_C_ID', '_B_ID'))
    if not df_merge_id.empty:
        for idx_c, idx_b in zip(df_merge_id['ID_Original_C_ID'], df_merge_id['ID_Original_B_ID']):
            df_c.loc[df_c['ID_Original'] == idx_c, 'Conciliado'] = True
            df_b.loc[df_b['ID_Original'] == idx_b, 'Conciliado'] = True
    rep_id = formatear_reporte_id(df_merge_id)

    # PASO 2: FECHA
    df_c_p = df_c[~df_c['Conciliado']].copy()
    df_b_p = df_b[~df_b['Conciliado']].copy()
    df_merge_f = pd.merge(df_c_p, df_b_p, on=['Fecha', 'Abs_Monto'], how='outer', suffixes=('_C', '_B'), indicator=True)
    rep_f = formatear_reporte_fecha(df_merge_f)
    
    if not rep_f.empty:
        for idx_c in df_merge_f[df_merge_f['_merge'] == 'both']['ID_Original_C']:
            df_c.loc[df_c['ID_Original'] == idx_c, 'Conciliado'] = True
        for idx_b in df_merge_f[df_merge_f['_merge'] == 'both']['ID_Original_B']:
            df_b.loc[df_b['ID_Original'] == idx_b, 'Conciliado'] = True

    # PASO 3: TOLERANCIA
    df_c_p2 = df_c[~df_c['Conciliado']].copy()
    df_b_p2 = df_b[~df_b['Conciliado']].copy()
    tol_list = []
    for _, rc in df_c_p2.iterrows():
        matches = df_b_p2[(df_b_p2['Abs_Monto'] == rc['Abs_Monto']) & 
                         (df_b_p2['Fecha'] >= rc['Fecha'] - timedelta(days=TOLERANCIA_DIAS)) & 
                         (df_b_p2['Fecha'] <= rc['Fecha'] + timedelta(days=TOLERANCIA_DIAS)) & 
                         (~df_b_p2['Conciliado'])]
        if not matches.empty:
            rb = matches.iloc[0]
            df_c.loc[df_c['ID_Original'] == rc['ID_Original'], 'Conciliado'] = True
            df_b.loc[df_b['ID_Original'] == rb['ID_Original'], 'Conciliado'] = True
            tol_list.append({'Estado': f'Conciliado (+/- {TOLERANCIA_DIAS} Días)', 'Fecha': rb['Fecha'], 
                             'Monto_C': rc['Monto'], 'Monto_B': rb['Monto'], 'Concepto_C': rc['Concepto'], 
                             'Concepto_B': rb['Concepto'], f'{ID_COL}_C': rc[ID_COL], f'{ID_COL}_B': rb[ID_COL]})
    rep_t = pd.DataFrame(tol_list, columns=get_columnas_finales())

    # PENDIENTES FINALES
    df_c_f = df_c[~df_c['Conciliado']].copy()
    df_b_f = df_b[~df_b['Conciliado']].copy()
    df_m_p = pd.merge(df_c_f, df_b_f, on='Abs_Monto', how='outer', suffixes=('_C', '_B'), indicator=True)
    rep_p = formatear_reporte_pendientes(df_m_p)

    df_final = pd.concat([rep_id, rep_f, rep_t, rep_p], ignore_index=True)
    return df_final.sort_values(by='Fecha').reset_index(drop=True)

# ==========================================================
# --- GENERADOR DE EXCEL PREMIUM (PASO 1) ---
# ==========================================================
@st.cache_data
def to_excel_premium(df, cliente="Empresa Cliente", periodo="Enero 2026"):
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='dd/mm/yyyy')
    workbook = writer.book
    
    # Formatos
    fmt_tit = workbook.add_format({'bold': True, 'font_size': 20, 'font_color': '#1E1B4B'})
    fmt_sub = workbook.add_format({'bold': True, 'font_size': 12, 'font_color': '#4F46E5'})
    fmt_head = workbook.add_format({'bold': True, 'bg_color': '#F1F5F9', 'border': 1})
    fmt_num = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
    
    col_v = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'border': 1})
    col_a = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500', 'border': 1})
    col_r = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'border': 1})

    # 1. CARÁTULA
    ws1 = workbook.add_worksheet('Certificado de Conciliación')
    ws1.hide_gridlines(2)
    ws1.write('B2', 'FINMATCH', fmt_tit)
    ws1.write('B3', 'Reporte Final de Conciliación Bancaria', fmt_sub)
    ws1.write('B5', f'CLIENTE: {cliente}'); ws1.write('B6', f'PERÍODO: {periodo}')
    ws1.write('B8', 'RESUMEN EJECUTIVO', workbook.add_format({'bold': True, 'underline': True}))
    
    res = df['Estado'].value_counts().reset_index()
    res.columns = ['Estado', 'Registros']
    ws1.write_row('B10', res.columns, fmt_head)
    for i, row in res.iterrows(): ws1.write_row(10+i, 1, row, workbook.add_format({'border':1}))

    # 2. DETALLE
    ws2 = workbook.add_worksheet('Reporte Detallado')
    df.to_excel(writer, sheet_name='Reporte Detallado', index=False)
    ws2.conditional_format(f'A2:H{len(df)+1}', {'type': 'text', 'criteria': 'containing', 'value': 'Conciliado', 'format': col_v})
    ws2.conditional_format(f'A2:H{len(df)+1}', {'type': 'text', 'criteria': 'containing', 'value': 'Contabilidad', 'format': col_a})
    ws2.conditional_format(f'A2:H{len(df)+1}', {'type': 'text', 'criteria': 'containing', 'value': 'Banco', 'format': col_r})
    ws2.set_column('C:D', 15, fmt_num); ws2.set_column('A:A', 30); ws2.set_column('E:H', 25)

    # 3. RESUMEN CONCEPTOS
    df_p = df[df['Estado'].str.contains('Pendiente')].copy()
    if not df_p.empty:
        ws3 = workbook.add_worksheet('Resumen de Conceptos')
        df_p['Concepto Final'] = df_p['Concepto_C'].fillna(df_p['Concepto_B'])
        df_ag = df_p.groupby(['Estado', 'Concepto Final'])[['Monto_C', 'Monto_B']].sum().reset_index()
        df_ag['Total'] = df_ag['Monto_C'].fillna(0) + df_ag['Monto_B'].fillna(0)
        df_ag[['Estado', 'Concepto Final', 'Total']].to_excel(writer, sheet_name='Resumen de Conceptos', index=False)
        ws3.set_column('A:B', 35); ws3.set_column('C:C', 15, fmt_num)

    writer.close()
    return output.getvalue()

# ==========================================================
# --- FRONTEND STREAMLIT ---
# ==========================================================
st.set_page_config(page_title="FinMatch | Conciliador Web", layout="centered")

if check_password():
    with st.sidebar:
        st.title("FINMATCH")
        st.button("🚪 Cerrar Sesión", on_click=logout)

    st.title("Conciliador Web 🏦")
    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1: up_c = st.file_uploader("Contabilidad", type=['xlsx'])
    with col2: up_b = st.file_uploader("Banco", type=['xlsx'])

    if st.button("▶️ EJECUTAR CONCILIACIÓN", type="primary", use_container_width=True):
        if up_c and up_b:
            with st.spinner("Procesando..."):
                dc = cargar_datos(up_c, 'Contable')
                db = cargar_datos(up_b, 'Banco')
                if dc is not None and db is not None:
                    rep = conciliar(dc, db)
                    st.success("✅ Conciliación Exitosa")
                    st.dataframe(rep['Estado'].value_counts())
                    
                    data_ex = to_excel_premium(rep)
                    st.download_button("⬇️ Descargar Reporte Final de Conciliación", data=data_ex, 
                                     file_name="Reporte_Final_FinMatch.xlsx", mime="application/vnd.ms-excel")

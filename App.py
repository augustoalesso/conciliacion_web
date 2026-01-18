import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import timedelta

# ==========================================================
# 🔒 LÓGICA DE ACCESO PRIVADO
# ==========================================================
try:
    VALID_USERNAME = st.secrets["username"]
    VALID_PASSWORD = st.secrets["password"]
except:
    VALID_USERNAME = "encargado"
    VALID_PASSWORD = "AugustoBot1"

def logout():
    st.session_state["password_correct"] = False
    st.rerun()

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    if st.session_state["password_correct"]:
        return True
    
    st.title("🔐 Acceso Restringido - FinMatch")
    with st.form(key="login_form"):
        u = st.text_input("Usuario")
        p = st.text_input("Contraseña", type="password")
        if st.form_submit_button("Ingresar"):
            if u == VALID_USERNAME and p == VALID_PASSWORD:
                st.session_state["password_correct"] = True
                st.rerun()
            else:
                st.error("❌ Credenciales incorrectas")
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

def get_columnas_finales():
    return ['Estado', 'Fecha', 'Monto_C', 'Monto_B', 'Concepto_C', 'Concepto_B', f'{ID_COL}_C', f'{ID_COL}_B']

@st.cache_data
def cargar_datos(uploaded_file, origen):
    try:
        df = pd.read_excel(uploaded_file)
        df = df.rename(columns={ex: internal for ex, internal in COLUMNAS_MAPEO.items() if ex in df.columns})
        if origen == 'Contable' and 'Debe' in df.columns:
            df['Monto'] = pd.to_numeric(df['Debe'], errors='coerce').fillna(0) - pd.to_numeric(df['Haber'], errors='coerce').fillna(0)
        else:
            df['Monto'] = pd.to_numeric(df['Monto'], errors='coerce').fillna(0)
        df['Abs_Monto'] = df['Monto'].abs()
        df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce', dayfirst=True)
        df[ID_COL] = df[ID_COL].astype(str).replace('nan', 'S/D')
        df['ID_Original'] = df.index
        return df[['Fecha', 'Monto', 'Abs_Monto', 'Concepto', ID_COL, 'ID_Original']]
    except Exception as e:
        st.error(f"Error en {origen}: {e}")
        return None

# ==========================================================
# --- MOTOR DE CONCILIACIÓN ---
# ==========================================================
def ejecutar_conciliacion(df_c, df_b):
    df_c['Conciliado'] = False
    df_b['Conciliado'] = False

    m1 = pd.merge(df_c, df_b, on=[ID_COL, 'Abs_Monto'], how='inner', suffixes=('_C', '_B'))
    for ic, ib in zip(m1['ID_Original_C'], m1['ID_Original_B']):
        df_c.loc[df_c['ID_Original'] == ic, 'Conciliado'] = True
        df_b.loc[df_b['ID_Original'] == ib, 'Conciliado'] = True
    rep1 = m1.assign(Estado='Conciliado por ID')

    df_c_p = df_c[~df_c['Conciliado']].copy()
    df_b_p = df_b[~df_b['Conciliado']].copy()
    m2 = pd.merge(df_c_p, df_b_p, on=['Fecha', 'Abs_Monto'], how='inner', suffixes=('_C', '_B'))
    for ic, ib in zip(m2['ID_Original_C'], m2['ID_Original_B']):
        df_c.loc[df_c['ID_Original'] == ic, 'Conciliado'] = True
        df_b.loc[df_b['ID_Original'] == ib, 'Conciliado'] = True
    rep2 = m2.assign(Estado='Conciliado por Fecha')

    df_c_p2 = df_c[~df_c['Conciliado']].copy()
    df_b_p2 = df_b[~df_b['Conciliado']].copy()
    t_list = []
    for _, rc in df_c_p2.iterrows():
        match = df_b_p2[(df_b_p2['Abs_Monto'] == rc['Abs_Monto']) & 
                        (df_b_p2['Fecha'] >= rc['Fecha'] - timedelta(days=TOLERANCIA_DIAS)) & 
                        (df_b_p2['Fecha'] <= rc['Fecha'] + timedelta(days=TOLERANCIA_DIAS)) & 
                        (~df_b_p2['Conciliado'])]
        if not match.empty:
            rb = match.iloc[0]
            df_c.loc[df_c['ID_Original'] == rc['ID_Original'], 'Conciliado'] = True
            df_b.loc[df_b['ID_Original'] == rb['ID_Original'], 'Conciliado'] = True
            t_list.append({'Estado': f'Conciliado por Tolerancia (±{TOLERANCIA_DIAS} días)', 'Fecha': rb['Fecha'], 'Monto_C': rc['Monto'], 'Monto_B': rb['Monto'], 'Concepto_C': rc['Concepto'], 'Concepto_B': rb['Concepto'], f'{ID_COL}_C': rc[ID_COL], f'{ID_COL}_B': rb[ID_COL]})
    rep3 = pd.DataFrame(t_list)

    rep4_c = df_c[~df_c['Conciliado']].rename(columns={'Monto':'Monto_C', 'Concepto':'Concepto_C', ID_COL: f'{ID_COL}_C'}).assign(Estado='Pendiente - Libro Contable')
    rep4_b = df_b[~df_b['Conciliado']].rename(columns={'Monto':'Monto_B', 'Concepto':'Concepto_B', ID_COL: f'{ID_COL}_B'}).assign(Estado='Pendiente - Extracto Bancario')
    
    final = pd.concat([rep1, rep2, rep3, rep4_c, rep4_b], ignore_index=True)
    final['Fecha'] = final['Fecha'].fillna(final['Fecha_B']).fillna(final['Fecha_C'])
    return final.reindex(columns=get_columnas_finales()).sort_values('Fecha')

# ==========================================================
# --- EXCEL PREMIUM (DISEÑO FINAL) ---
# ==========================================================
def to_excel_premium(df, cliente=""):
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='dd/mm/yyyy')
    workbook = writer.book

    # --- FORMATOS ---
    f_tit = workbook.add_format({'bold': True, 'font_size': 22, 'font_color': '#1E1B4B'})
    f_sub = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#4F46E5'})
    f_head = workbook.add_format({'bold': True, 'bg_color': '#D9D9D9', 'border': 1, 'align': 'center'})
    f_num = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
    f_std = workbook.add_format({'border': 1})
    
    c_verde = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'border': 1})
    c_azul = workbook.add_format({'bg_color': '#DDEBF7', 'font_color': '#000000', 'border': 1})
    c_amarillo = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500', 'border': 1})
    c_rojo = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'border': 1})
    c_violeta = workbook.add_format({'bg_color': '#E0BBE4', 'font_color': '#5D0970', 'border': 1})

    # --- HOJA 1: CARÁTULA ---
    ws0 = workbook.add_worksheet('Certificado FinMatch')
    ws0.hide_gridlines(2)
    ws0.write('B2', 'FINMATCH - CONCILIADOR WEB', f_tit)
    ws0.write('B3', 'Reporte de Auditoría de Conciliación Bancaria', f_sub)
    ws0.write('B5', 'CLIENTE:', workbook.add_format({'bold': True})); ws0.write('C5', cliente.upper())
    
    res = df['Estado'].value_counts().reset_index()
    res.columns = ['Estado', 'Total']
    ws0.write_row('B8', res.columns, f_head)
    for i, row in res.iterrows():
        txt = row['Estado']
        fmt = c_verde if 'ID' in txt or 'Fecha' in txt else c_azul if 'Tolerancia' in txt else c_amarillo if 'Contable' in txt else c_rojo
        ws0.write_row(8+i, 1, row, fmt)

    # --- HOJA 2: REPORTE DETALLE ---
    ws1 = workbook.add_worksheet('Reporte Detallado')
    for col_num, value in enumerate(df.columns.values):
        ws1.write(0, col_num, value, f_head)
    
    for row_num, row_data in enumerate(df.values):
        for col_num, cell_value in enumerate(row_data):
            fmt = f_num if col_num in [2, 3] else f_std
            ws1.write(row_num + 1, col_num, cell_value, fmt)

    rows = len(df)
    ws1.conditional_format(1, 0, rows, 0, {'type': 'text', 'criteria': 'containing', 'value': 'ID', 'format': c_verde})
    ws1.conditional_format(1, 0, rows, 0, {'type': 'text', 'criteria': 'containing', 'value': 'Fecha', 'format': c_verde})
    ws1.conditional_format(1, 0, rows, 0, {'type': 'text', 'criteria': 'containing', 'value': 'Tolerancia', 'format': c_azul})
    ws1.conditional_format(1, 0, rows, 0, {'type': 'text', 'criteria': 'containing', 'value': 'Contable', 'format': c_amarillo})
    ws1.conditional_format(1, 0, rows, 0, {'type': 'text', 'criteria': 'containing', 'value': 'Bancario', 'format': c_rojo})
    
    ws1.set_column('A:A', 35); ws1.set_column('B:B', 12); ws1.set_column('E:H', 25)

    # --- HOJA 3: RESUMEN CONCEPTOS ---
    df_p = df[df['Estado'].str.contains('Pendiente')].copy()
    if not df_p.empty:
        ws2 = workbook.add_worksheet('Resumen Conceptos')
        df_p['Concepto Final'] = df_p['Concepto_C'].fillna(df_p['Concepto_B'])
        ag = df_p.groupby(['Estado', 'Concepto Final'])[['Monto_C', 'Monto_B']].sum().reset_index()
        ag['Total'] = ag['Monto_C'].fillna(0) + ag['Monto_B'].fillna(0)
        abs_totals = ag['Total'].abs()
        duplicados = ag[abs_totals.duplicated(keep=False)]['Total'].abs().unique()
        ag['Control'] = np.where(ag['Total'].abs().isin(duplicados), 'Posible Coincidencia', '')
        
        columnas_ag = ['Estado', 'Concepto Final', 'Total', 'Control']
        for col_num, value in enumerate(columnas_ag):
            ws2.write(0, col_num, value, f_head)
        
        for row_num, row_data in enumerate(ag[columnas_ag].values):
            for col_num, cell_value in enumerate(row_data):
                fmt = f_num if col_num == 2 else f_std
                ws2.write(row_num + 1, col_num, cell_value, fmt)
        
        rows2 = len(ag)
        ws2.conditional_format(1, 0, rows2, 0, {'type': 'text', 'criteria': 'containing', 'value': 'Contable', 'format': c_amarillo})
        ws2.conditional_format(1, 0, rows2, 0, {'type': 'text', 'criteria': 'containing', 'value': 'Bancario', 'format': c_rojo})
        ws2.conditional_format(1, 3, rows2, 3, {'type': 'text', 'criteria': 'containing', 'value': 'Posible', 'format': c_violeta})
        ws2.set_column('A:A', 35); ws2.set_column('B:B', 45); ws2.set_column('D:D', 20)

    writer.close()
    return output.getvalue()

# ==========================================================
# --- FRONTEND ---
# ==========================================================
st.set_page_config(page_title="FinMatch - ConciliadorWeb")
if check_password():
    st.title("Sistema ConciliadorWeb 🏦")
    cliente = st.text_input("Nombre de la Empresa Cliente", "Distribuidora S.A.")
    c1, c2 = st.columns(2)
    with c1: up_c = st.file_uploader("Archivo Contabilidad", type=['xlsx'])
    with c2: up_b = st.file_uploader("Archivo Banco", type=['xlsx'])

    if st.button("▶️ EJECUTAR CONCILIACIÓN", type="primary"):
        if up_c and up_b:
            dc, db = cargar_datos(up_c, 'Contable'), cargar_datos(up_b, 'Banco')
            if dc is not None and db is not None:
                res = ejecutar_conciliacion(dc, db)
                st.success("✅ Auditoría completada con éxito")
                st.download_button(
                    label="⬇️ Descargar FinMatch_Reporte.xlsx",
                    data=to_excel_premium(res, cliente),
                    file_name="FinMatch_Reporte.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

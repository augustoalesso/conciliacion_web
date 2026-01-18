import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import timedelta

# ==========================================================
# 🔒 ACCESO PRIVADO (Secrets en Streamlit Cloud)
# ==========================================================
def check_password():
    def password_entered():
        if (st.session_state["username"] == st.secrets["username"] and 
            st.session_state["password"] == st.secrets["password"]):
            st.session_state["password_correct"] = True
            del st.session_state["password"]
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.title("🔐 Acceso Restringido - FinMatch")
        st.text_input("Usuario", key="username")
        st.text_input("Contraseña", type="password", key="password")
        st.button("Ingresar", on_click=password_entered)
        return False
    elif not st.session_state["password_correct"]:
        st.title("🔐 Acceso Restringido - FinMatch")
        st.text_input("Usuario", key="username")
        st.text_input("Contraseña", type="password", key="password")
        st.button("Ingresar", on_click=password_entered)
        st.error("❌ Usuario o contraseña incorrectos")
        return False
    return True

# ==========================================================
# --- CONFIGURACIÓN TÉCNICA ---
# ==========================================================
TOLERANCIA_DIAS = 3 
ID_COL = 'Numero Operacion ID' 

COLUMNAS_MAPEO = {
    'Fecha': 'Fecha', 'Debe': 'Debe', 'Haber': 'Haber',
    'Monto': 'Monto', 'Importe': 'Monto', 'Concepto': 'Concepto', 
    'Descripción': 'Concepto', 'Numero de operación': ID_COL,
    'Referencia': ID_COL, 'Nro de Comprobante': ID_COL
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
        if ID_COL not in df.columns: df[ID_COL] = "S/D"
        df[ID_COL] = df[ID_COL].astype(str)
        df['ID_Original'] = df.index
        return df[['Fecha', 'Monto', 'Abs_Monto', 'Concepto', ID_COL, 'ID_Original']]
    except Exception as e:
        st.error(f"Error en {origen}: {e}")
        return None

# ==========================================================
# --- MOTOR DE CONCILIACIÓN REFORZADO ---
# ==========================================================
def ejecutar_conciliacion(df_c, df_b):
    df_c['Conciliado'] = False
    df_b['Conciliado'] = False

    # Paso 1: ID Exacto
    m1 = pd.merge(df_c, df_b, on=[ID_COL, 'Abs_Monto'], how='inner', suffixes=('_C', '_B'))
    for ic, ib in zip(m1['ID_Original_C'], m1['ID_Original_B']):
        df_c.loc[df_c['ID_Original'] == ic, 'Conciliado'] = True
        df_b.loc[df_b['ID_Original'] == ib, 'Conciliado'] = True
    
    rep1 = m1.copy()
    rep1['Estado'] = 'Conciliado por ID'
    rep1['Fecha'] = rep1['Fecha_B'].fillna(rep1['Fecha_C'])
    rep1 = rep1.reindex(columns=get_columnas_finales())

    # Paso 2: Fecha Exacta
    df_c_p = df_c[~df_c['Conciliado']].copy()
    df_b_p = df_b[~df_b['Conciliado']].copy()
    rep2 = pd.DataFrame(columns=get_columnas_finales())
    
    if not df_c_p.empty and not df_b_p.empty:
        m2 = pd.merge(df_c_p, df_b_p, on=['Fecha', 'Abs_Monto'], how='inner', suffixes=('_C', '_B'))
        if not m2.empty:
            for ic, ib in zip(m2['ID_Original_C'], m2['ID_Original_B']):
                df_c.loc[df_c['ID_Original'] == ic, 'Conciliado'] = True
                df_b.loc[df_b['ID_Original'] == ib, 'Conciliado'] = True
            rep2 = m2.copy()
            rep2['Estado'] = 'Conciliado por Fecha'
            rep2['Fecha'] = rep2['Fecha_B'].fillna(rep2['Fecha_C'])
            rep2 = rep2.reindex(columns=get_columnas_finales())

    # Paso 3: Tolerancia
    df_c_p2 = df_c[~df_c['Conciliado']].copy()
    df_b_p2 = df_b[~df_b['Conciliado']].copy()
    t_list = []
    for _, rc in df_c_p2.iterrows():
        matches = df_b_p2[(df_b_p2['Abs_Monto'] == rc['Abs_Monto']) & 
                         (df_b_p2['Fecha'] >= rc['Fecha'] - timedelta(days=TOLERANCIA_DIAS)) & 
                         (df_b_p2['Fecha'] <= rc['Fecha'] + timedelta(days=TOLERANCIA_DIAS)) & 
                         (~df_b_p2['Conciliado'])]
        if not matches.empty:
            rb = matches.iloc[0]
            df_c.loc[df_c['ID_Original'] == rc['ID_Original'], 'Conciliado'] = True
            df_b.loc[df_b['ID_Original'] == rb['ID_Original'], 'Conciliado'] = True
            t_list.append({'Estado': f'Conciliado (+/- {TOLERANCIA_DIAS} Días)', 'Fecha': rb['Fecha'], 
                          'Monto_C': rc['Monto'], 'Monto_B': rb['Monto'], 'Concepto_C': rc['Concepto'], 
                          'Concepto_B': rb['Concepto'], f'{ID_COL}_C': rc[ID_COL], f'{ID_COL}_B': rb[ID_COL]})
    rep3 = pd.DataFrame(t_list, columns=get_columnas_finales())

    # Pendientes
    c_f = df_c[~df_c['Conciliado']].copy()
    b_f = df_b[~df_b['Conciliado']].copy()
    rep4_c = c_f.rename(columns={'Monto':'Monto_C', 'Concepto':'Concepto_C', ID_COL: f'{ID_COL}_C', 'Fecha':'Fecha_C'})
    rep4_c['Estado'] = 'Pendiente - Solo en Contabilidad'
    rep4_b = b_f.rename(columns={'Monto':'Monto_B', 'Concepto':'Concepto_B', ID_COL: f'{ID_COL}_B', 'Fecha':'Fecha_B'})
    rep4_b['Estado'] = 'Pendiente - Solo en Banco'
    
    rep4_c['Fecha'] = rep4_c['Fecha_C']
    rep4_b['Fecha'] = rep4_b['Fecha_B']

    final = pd.concat([rep1, rep2, rep3, rep4_c, rep4_b], ignore_index=True)
    final['Fecha'] = pd.to_datetime(final['Fecha'], errors='coerce')
    return final.reindex(columns=get_columnas_finales()).sort_values('Fecha', na_position='last')

# ==========================================================
# --- GENERADOR DE EXCEL PREMIUM CON COLORES ---
# ==========================================================
def to_excel_premium(df):
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='dd/mm/yyyy')
    workbook = writer.book
    
    # FORMATOS DE COLORES RECUPERADOS
    f_tit = workbook.add_format({'bold': True, 'font_size': 22, 'font_color': '#1E1B4B'})
    f_sub = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#4F46E5'})
    f_num = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
    f_head = workbook.add_format({'bold': True, 'bg_color': '#F1F5F9', 'border': 1})
    f_bord = workbook.add_format({'border': 1})
    
    c_verde = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'border': 1})
    c_amarillo = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500', 'border': 1})
    c_rojo = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'border': 1})

    # 1. Carátula
    ws1 = workbook.add_worksheet('Certificado')
    ws1.hide_gridlines(2)
    ws1.write('B2', 'FINMATCH', f_tit)
    ws1.write('B3', 'Reporte Final de Conciliación Bancaria', f_sub)
    ws1.write('B5', 'RESUMEN EJECUTIVO', workbook.add_format({'bold': True, 'underline': True}))
    res = df['Estado'].value_counts().reset_index()
    res.columns = ['Estado', 'Registros']
    ws1.write_row('B7', res.columns, f_head)
    for i, row in res.iterrows(): ws1.write_row(7+i, 1, row, f_bord)

    # 2. Detalle con Colores
    ws2 = workbook.add_worksheet('Reporte Detallado')
    df.to_excel(writer, sheet_name='Reporte Detallado', index=False)
    
    rango = f'A2:H{len(df)+1}'
    ws2.conditional_format(rango, {'type': 'text', 'criteria': 'containing', 'value': 'Conciliado', 'format': c_verde})
    ws2.conditional_format(rango, {'type': 'text', 'criteria': 'containing', 'value': 'Contabilidad', 'format': c_amarillo})
    ws2.conditional_format(rango, {'type': 'text', 'criteria': 'containing', 'value': 'Banco', 'format': c_rojo})
    
    ws2.set_column('C:D', 18, f_num); ws2.set_column('A:A', 35); ws2.set_column('E:H', 25)

    # 3. Resumen Conceptos
    df_p = df[df['Estado'].str.contains('Pendiente')].copy()
    if not df_p.empty:
        ws3 = workbook.add_worksheet('Resumen de Conceptos')
        df_p['Concepto Final'] = df_p['Concepto_C'].fillna(df_p['Concepto_B'])
        ag = df_p.groupby(['Estado', 'Concepto Final'])[['Monto_C', 'Monto_B']].sum().reset_index()
        ag['Total'] = ag['Monto_C'].fillna(0) + ag['Monto_B'].fillna(0)
        ag[['Estado', 'Concepto Final', 'Total']].to_excel(writer, sheet_name='Resumen de Conceptos', index=False)
        ws3.set_column('B:B', 45); ws3.set_column('C:C', 18, f_num)

    writer.close()
    return output.getvalue()

# ==========================================================
# --- FRONTEND ---
# ==========================================================
st.set_page_config(page_title="FinMatch")
if check_password():
    st.title("Conciliador Web 🏦")
    c1, c2 = st.columns(2)
    with c1: up_c = st.file_uploader("Contabilidad", type=['xlsx'])
    with c2: up_b = st.file_uploader("Banco", type=['xlsx'])

    if st.button("▶️ EJECUTAR"):
        if up_c and up_b:
            with st.spinner("Procesando..."):
                dc, db = cargar_datos(up_c, 'Contable'), cargar_datos(up_b, 'Banco')
                if dc is not None and db is not None:
                    res = ejecutar_conciliacion(dc, db)
                    st.success("Hecho")
                    st.download_button("Descargar Reporte Premium", to_excel_premium(res), "Reporte_FinMatch.xlsx")

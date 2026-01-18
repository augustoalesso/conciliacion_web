import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import timedelta

# ==========================================================
# 🔒 LÓGICA DE ACCESO PRIVADO (Ajustada para st.secrets)
# ==========================================================
def check_password():
    """Muestra el formulario de login y verifica las credenciales."""
    def password_entered():
        # Compara con los secretos cargados en el panel de Streamlit Cloud
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
        st.error("❌ Usuario o Contraseña incorrecta.")
        return False
    return True

def logout():
    st.session_state["password_correct"] = False
    st.rerun()

# ==========================================================
# --- CONFIGURACIÓN ESTÁTICA Y MAPPING ---
# ==========================================================
TOLERANCIA_DIAS = 3 
ID_COL = 'Numero Operacion ID' 

COLUMNAS_MAPEO = {
    'Fecha': 'Fecha', 
    'Debe': 'Debe',          
    'Haber': 'Haber',      
    'Monto': 'Monto',        
    'Concepto': 'Concepto',
    'Numero de operación': ID_COL 
}

# --- Funciones Auxiliares de Formateo y Carga ---

def get_columnas_finales():
    return ['Estado', 'Fecha', 'Monto_C', 'Monto_B', 'Concepto_C', 'Concepto_B', f'{ID_COL}_C', f'{ID_COL}_B']

def formatear_reporte_id(df_merge_id):
    COLUMNAS_RENOMBRAR = {f'Monto_C_ID': 'Monto_C', f'Monto_B_ID': 'Monto_B',
        f'Concepto_C_ID': 'Concepto_C', f'Concepto_B_ID': 'Concepto_B',
        f'{ID_COL}_C_ID': f'{ID_COL}_C', f'{ID_COL}_B_ID': f'{ID_COL}_B'}
    df_reporte = df_merge_id.rename(columns=COLUMNAS_RENOMBRAR)
    df_reporte['Estado'] = 'Conciliado por ID'
    df_reporte['Fecha'] = df_reporte['Fecha_B_ID'].fillna(df_reporte['Fecha_C_ID']) 
    return df_reporte.reindex(columns=get_columnas_finales())

def formatear_reporte_fecha(df_merge_fecha):
    df_reporte = df_merge_fecha.copy()
    df_reporte['Estado'] = np.select(
        [df_reporte['_merge'] == 'both'],
        ['Conciliado por Fecha'],
        default='Error'
    )
    df_conciliado = df_reporte[df_reporte['_merge'] == 'both'].copy()
    COLUMNAS_RENOMBRAR = {f'Monto_C': 'Monto_C', f'Monto_B': 'Monto_B', 
        f'Concepto_C': 'Concepto_C', f'Concepto_B': 'Concepto_B',
        f'{ID_COL}_C': f'{ID_COL}_C', f'{ID_COL}_B': f'{ID_COL}_B'}
    df_conciliado = df_conciliado.rename(columns=COLUMNAS_RENOMBRAR)
    return df_conciliado.reindex(columns=get_columnas_finales())

def formatear_reporte_pendientes(df_final_pendientes):
    df_trabajo = df_final_pendientes.copy()
    df_trabajo['Estado'] = np.select(
        [df_trabajo['_merge'] == 'left_only', df_trabajo['_merge'] == 'right_only'],
        ['Pendiente - Solo en Contabilidad', 'Pendiente - Solo en Banco'],
        default='Error'
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
        df = df.drop(columns=['Debe', 'Haber'], errors='ignore') 
    elif 'Monto' in df.columns:
        df['Monto'] = pd.to_numeric(df['Monto'], errors='coerce').fillna(0) 
    
    df['Abs_Monto'] = df['Monto'].abs() 
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce', dayfirst=True) 
    df[ID_COL] = df[ID_COL].astype(str)
    df['ID_Original'] = df.index
    df['Origen'] = origen
    return df[['Fecha', 'Monto', 'Abs_Monto', 'Concepto', ID_COL, 'ID_Original', 'Origen']]

@st.cache_data
def conciliar(df_contable, df_bancario):
    columnas_finales = get_columnas_finales()
    df_reporte_id = pd.DataFrame(columns=columnas_finales)
    df_reporte_fecha = pd.DataFrame(columns=columnas_finales)
    df_reporte_tolerancia = pd.DataFrame(columns=columnas_finales) 
    
    df_c = df_contable.copy()
    df_b = df_bancario.copy()
    df_c['Conciliado'] = False
    df_b['Conciliado'] = False

    # PASO 1: ID
    df_merge_id = pd.merge(df_c, df_b, on=[ID_COL, 'Abs_Monto'], how='inner', suffixes=('_C_ID', '_B_ID'))
    if not df_merge_id.empty:
        for index_c, index_b in zip(df_merge_id['ID_Original_C_ID'], df_merge_id['ID_Original_B_ID']):
            df_c.loc[df_c['ID_Original'] == index_c, 'Conciliado'] = True
            df_b.loc[df_b['ID_Original'] == index_b, 'Conciliado'] = True
        df_reporte_id = formatear_reporte_id(df_merge_id)

    # PASO 2: FECHA
    df_c_p = df_c[df_c['Conciliado'] == False].copy().reset_index(drop=True)
    df_b_p = df_b[df_b['Conciliado'] == False].copy().reset_index(drop=True)
    if not df_c_p.empty or not df_b_p.empty:
        df_merge_fecha = pd.merge(df_c_p, df_b_p, on=['Fecha', 'Abs_Monto'], how='outer', suffixes=('_C', '_B'), indicator=True)
        df_conc_f = df_merge_fecha[df_merge_fecha['_merge'] == 'both'].copy()
        if not df_conc_f.empty:
            df_reporte_fecha = formatear_reporte_fecha(df_conc_f)
            for ic in df_conc_f['ID_Original_C'].dropna(): df_c.loc[df_c['ID_Original'] == ic, 'Conciliado'] = True
            for ib in df_conc_f['ID_Original_B'].dropna(): df_b.loc[df_b['ID_Original'] == ib, 'Conciliado'] = True

    # PASO 3: TOLERANCIA
    df_c_p2 = df_c[df_c['Conciliado'] == False].copy().reset_index(drop=True)
    df_b_p2 = df_b[df_b['Conciliado'] == False].copy().reset_index(drop=True)
    tol_list = []
    if not df_c_p2.empty and not df_b_p2.empty:
        for _, rc in df_c_p2.iterrows():
            matches = df_b_p2[(df_b_p2['Abs_Monto'] == rc['Abs_Monto']) & 
                             (df_b_p2['Fecha'] >= rc['Fecha'] - timedelta(days=TOLERANCIA_DIAS)) & 
                             (df_b_p2['Fecha'] <= rc['Fecha'] + timedelta(days=TOLERANCIA_DIAS)) & 
                             (df_b_p2['Conciliado'] == False)]
            if not matches.empty:
                rb = matches.iloc[0]
                df_c.loc[df_c['ID_Original'] == rc['ID_Original'], 'Conciliado'] = True
                df_b.loc[df_b['ID_Original'] == rb['ID_Original'], 'Conciliado'] = True
                tol_list.append({'Estado': f'Conciliado (+/- {TOLERANCIA_DIAS} Días)', 'Fecha': rb['Fecha'], 
                                'Monto_C': rc['Monto'], 'Monto_B': rb['Monto'], 'Concepto_C': rc['Concepto'], 
                                'Concepto_B': rb['Concepto'], f'{ID_COL}_C': rc[ID_COL], f'{ID_COL}_B': rb[ID_COL]})
    df_reporte_tolerancia = pd.DataFrame(tol_list, columns=columnas_finales)

    # PENDIENTES
    df_c_f = df_c[df_c['Conciliado'] == False].copy()
    df_b_f = df_b[df_b['Conciliado'] == False].copy()
    df_m_p = pd.merge(df_c_f, df_b_f, on='Abs_Monto', how='outer', suffixes=('_C', '_B'), indicator=True)
    df_reporte_pendientes = formatear_reporte_pendientes(df_m_p)
    
    df_reporte = pd.concat([df_reporte_id, df_reporte_fecha, df_reporte_tolerancia, df_reporte_pendientes], ignore_index=True)
    return df_reporte.sort_values(by='Fecha').reset_index(drop=True)

@st.cache_data
def to_excel_with_summary(df):
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='dd/mm/yyyy')
    workbook = writer.book
    
    # --- Formatos FinMatch ---
    fmt_tit = workbook.add_format({'bold': True, 'font_size': 20, 'font_color': '#1E1B4B'})
    fmt_sub = workbook.add_format({'bold': True, 'font_size': 12, 'font_color': '#4F46E5'})
    c_verde = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'border': 1})
    c_amarillo = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500', 'border': 1})
    c_rojo = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'border': 1})
    fmt_num = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
    fmt_head = workbook.add_format({'bold': True, 'bg_color': '#F1F5F9', 'border': 1})

    # Hoja 1: Carátula y Detalle
    ws1 = workbook.add_worksheet('Reporte Conciliación')
    ws1.hide_gridlines(2)
    ws1.write('A1', 'FINMATCH', fmt_tit)
    ws1.write('A2', 'Reporte Final de Conciliación Bancaria', fmt_sub)
    
    # Resumen Ejecutivo
    res_data = df['Estado'].value_counts().reset_index()
    res_data.columns = ['Estado', 'Total']
    ws1.write_row('A4', res_data.columns, fmt_head)
    for i, row in res_data.iterrows(): ws1.write_row(4+i, 0, row, workbook.add_format({'border': 1}))
    
    # Detalle con Colores
    start_row = 7 + len(res_data)
    df.to_excel(writer, sheet_name='Reporte Conciliación', startrow=start_row, index=False)
    rango = f'A{start_row+2}:H{start_row + 1 + len(df)}'
    ws1.conditional_format(rango, {'type': 'text', 'criteria': 'containing', 'value': 'Conciliado', 'format': c_verde})
    ws1.conditional_format(rango, {'type': 'text', 'criteria': 'containing', 'value': 'Solo en Contabilidad', 'format': c_amarillo})
    ws1.conditional_format(rango, {'type': 'text', 'criteria': 'containing', 'value': 'Solo en Banco', 'format': c_rojo})
    ws1.set_column('C:D', 15, fmt_num); ws1.set_column('A:A', 30); ws1.set_column('E:H', 22)

    # Hoja 2: Resumen Conceptos
    df_p = df[df['Estado'].str.contains('Pendiente')]
    if not df_p.empty:
        ws2 = workbook.add_worksheet('Resumen Conceptos')
        df_p['Conc_F'] = df_p['Concepto_C'].fillna(df_p['Concepto_B'])
        ag = df_p.groupby(['Estado', 'Conc_F'])[['Monto_C', 'Monto_B']].sum().reset_index()
        ag['Total'] = ag['Monto_C'].fillna(0) + ag['Monto_B'].fillna(0)
        ag[['Estado', 'Conc_F', 'Total']].to_excel(writer, sheet_name='Resumen Conceptos', index=False)
        ws2.set_column('B:B', 40); ws2.set_column('C:C', 15, fmt_num)

    writer.close()
    return output.getvalue()

# ==========================================================
# --- FRONTEND STREAMLIT ---
# ==========================================================
st.set_page_config(page_title="FinMatch | Conciliador Web", layout="centered")

if check_password():
    with st.sidebar:
        st.markdown("### Opciones")
        st.button("🚪 Cerrar Sesión", on_click=logout)

    st.title("Conciliador Web 🏦")
    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1: up_c = st.file_uploader("Contabilidad", type=['xlsx'])
    with c2: up_b = st.file_uploader("Banco", type=['xlsx'])

    if st.button("▶️ EJECUTAR CONCILIACIÓN", type="primary", use_container_width=True):
        if up_c and up_b:
            with st.spinner("Procesando..."):
                dc, db = cargar_datos(up_c, 'Contable'), cargar_datos(up_b, 'Banco')
                if dc is not None and db is not None:
                    resultado = conciliar(dc, db)
                    st.success("✅ Hecho")
                    st.dataframe(resultado['Estado'].value_counts())
                    st.download_button("⬇️ Descargar Reporte Final", to_excel_with_summary(resultado), "Reporte_FinMatch.xlsx")

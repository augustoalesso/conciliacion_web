import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import timedelta

# ==========================================================
# 🔒 LÓGICA DE ACCESO PRIVADO Y CONFIGURACIÓN SEGURA
# ==========================================================

# 🛑 1. DEFINIR TUS CREDENCIALES (SE LEEN DE UN ARCHIVO SECRETO)
# Si no puede leer las credenciales del archivo secreto de Streamlit Cloud, las variables estarán vacías.
try:
    VALID_USERNAME = st.secrets["db_credentials"]["username"]
    VALID_PASSWORD = st.secrets["db_credentials"]["password"]
except KeyError:
    # Esto evita que el código falle si el archivo secreto aún no existe en la nube
    VALID_USERNAME = ""
    VALID_PASSWORD = ""

def check_password():
    """Muestra el formulario de login y verifica las credenciales."""
    
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    # Si las credenciales no están cargadas (porque el archivo secreto no existe aún), mostramos un mensaje de error.
    if not VALID_USERNAME or not VALID_PASSWORD:
        st.error("❌ Error de Configuración: La aplicación no ha encontrado las credenciales seguras (st.secrets).")
        st.stop()
        
    if st.session_state["password_correct"]:
        return True

    # Mostrar formulario de login
    st.title("🔐 Acceso Restringido")
    st.markdown("---")
    
    with st.form(key="login_form"):
        username = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")
        login_button = st.form_submit_button("Ingresar")

    if login_button:
        # Aquí la aplicación compara el input del usuario con las variables leídas desde st.secrets
        if username == VALID_USERNAME and password == VALID_PASSWORD:
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
    'Fecha': 'Fecha', 
    'Debe': 'Debe',         
    'Haber': 'Haber',      
    'Monto': 'Monto',       
    'Concepto': 'Concepto',
    'Numero de operación': ID_COL 
}

# -------------------------------------------------------------------------------------------------------------------------------------------------
# --- FUNCIONES AUXILIARES Y DE PREPARACIÓN DE DATOS ---
# -------------------------------------------------------------------------------------------------------------------------------------------------

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
    df_conciliado = df_reporte[df_reporte['Estado'] != 'Error'].copy()
    
    COLUMNAS_RENOMBRAR = {f'Monto_C': 'Monto_C', f'Monto_B': 'Monto_B', 
        f'Concepto_C': 'Concepto_C', f'Concepto_B': 'Concepto_B',
        f'{ID_COL}_C': f'{ID_COL}_C', f'{ID_COL}_B': f'{ID_COL}_B'}
    df_conciliado = df_conciliado.rename(columns=COLUMNAS_RENOMBRAR)
    df_conciliado['Fecha'] = df_conciliado['Fecha'] 
    return df_conciliado.reindex(columns=get_columnas_finales())

def formatear_reporte_pendientes(df_final_pendientes):
    df_trabajo = df_final_pendientes.copy()
    
    df_trabajo['Estado'] = np.select(
        [
            df_trabajo['_merge'] == 'left_only',
            df_trabajo['_merge'] == 'right_only'
        ],
        [
            'Pendiente - Solo en Contabilidad',
            'Pendiente - Solo en Banco'
        ],
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
    """Carga y prepara un archivo subido a un DataFrame."""
    try:
        df = pd.read_excel(uploaded_file) 
    except Exception as e:
        st.error(f"Error al cargar el archivo de {origen}: {e}")
        return None
    
    # 1. Renombrar columnas
    df = df.rename(columns={excel_name: internal_name for excel_name, internal_name in COLUMNAS_MAPEO.items() if excel_name in df.columns})
    
    # --- LÓGICA DE UNIFICACIÓN DE MONTO (DEBE/HABER) ---
    if origen == 'Contable' and 'Debe' in df.columns and 'Haber' in df.columns:
        df['Debe'] = pd.to_numeric(df['Debe'], errors='coerce').fillna(0)
        df['Haber'] = pd.to_numeric(df['Haber'], errors='coerce').fillna(0)
        
        df['Monto'] = df['Debe'] - df['Haber'] 
        df = df.drop(columns=['Debe', 'Haber'], errors='ignore') 
        
    elif 'Monto' in df.columns:
        df['Monto'] = pd.to_numeric(df['Monto'], errors='coerce').fillna(0) 
    else:
        st.error(f"Error en {origen}: No se encontró la estructura de 'Debe'/'Haber' o la columna 'Monto' requerida.")
        return None

    # 3. Resto de la preparación
    df['Abs_Monto'] = df['Monto'].abs() 
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce', dayfirst=True) 
    
    # 4. Crear identificadores
    for col_int in ['Fecha', 'Monto', ID_COL]:
        if col_int not in df.columns:
            df[col_int] = np.nan if col_int == 'Fecha' else 'N/A'
            
    df[ID_COL] = df[ID_COL].astype(str)
            
    df['ID_Original'] = df.index
    df['Origen'] = origen
    
    return df[['Fecha', 'Monto', 'Abs_Monto', 'Concepto', ID_COL, 'ID_Original', 'Origen']]

# -------------------------------------------------------------------------------------------------------------------------------------------------
# --- FUNCIÓN CENTRAL DE CONCILIACIÓN (TRIPLE PASO) ---
# -------------------------------------------------------------------------------------------------------------------------------------------------

@st.cache_data
def conciliar(df_contable, df_bancario):
    """Realiza la conciliación en tres pasos."""
    
    columnas_finales = get_columnas_finales()
    df_reporte_id = pd.DataFrame(columns=columnas_finales)
    df_reporte_fecha = pd.DataFrame(columns=columnas_finales)
    df_reporte_tolerancia = pd.DataFrame(columns=columnas_finales) 
    
    df_c = df_contable.copy()
    df_b = df_bancario.copy()
    df_c['Conciliado'] = False
    df_b['Conciliado'] = False

    # PASO 1: CONCILIACIÓN EXACTA POR ID y MONTO
    df_merge_id = pd.merge(df_c, df_b, on=[ID_COL, 'Abs_Monto'], how='inner', suffixes=('_C_ID', '_B_ID'))
    
    if not df_merge_id.empty:
        INDEX_C = 'ID_Original_C_ID'
        INDEX_B = 'ID_Original_B_ID'
        
        for index_c, index_b in zip(df_merge_id[INDEX_C], df_merge_id[INDEX_B]):
            df_c.loc[df_c['ID_Original'] == index_c, 'Conciliado'] = True
            df_b.loc[df_b['ID_Original'] == index_b, 'Conciliado'] = True

        df_reporte_id = formatear_reporte_id(df_merge_id)

    # Preparar DataFrames Pendientes después del Paso 1
    df_c_pendiente = df_c[df_c['Conciliado'] == False].copy().reset_index(drop=True)
    df_b_pendiente = df_b[df_b['Conciliado'] == False].copy().reset_index(drop=True)

    
    # PASO 2: CONCILIACIÓN DE LOS RESTANTES por FECHA EXACTA y MONTO
    if not df_c_pendiente.empty or not df_b_pendiente.empty:
        df_merge_fecha = pd.merge(
            df_c_pendiente, df_b_pendiente, on=['Fecha', 'Abs_Monto'], how='outer', suffixes=('_C', '_B'), indicator=True
        )
        
        df_conciliado_fecha = df_merge_fecha[df_merge_fecha['_merge'] == 'both'].copy()
        
        if not df_conciliado_fecha.empty:
            df_reporte_fecha = formatear_reporte_fecha(df_conciliado_fecha)
            
            INDEX_C = 'ID_Original_C'
            INDEX_B = 'ID_Original_B'

            for index_c in df_conciliado_fecha[INDEX_C].dropna():
                df_c.loc[df_c['ID_Original'] == index_c, 'Conciliado'] = True
            for index_b in df_conciliado_fecha[INDEX_B].dropna():
                df_b.loc[df_b['ID_Original'] == index_b, 'Conciliado'] = True
        
            df_c_pendiente = df_c[df_c['Conciliado'] == False].copy().reset_index(drop=True)
            df_b_pendiente = df_b[df_b['Conciliado'] == False].copy().reset_index(drop=True)

    
    # PASO 3: CONCILIACIÓN DE LOS ÚLTIMOS RESTANTES por +/- N DÍAS
    
    conciliados_tolerancia_list = []
    
    if not df_c_pendiente.empty and not df_b_pendiente.empty:
        
        for index_c, row_c in df_c_pendiente.iterrows():
            if row_c['Conciliado']: continue
                
            monto_abs = row_c['Abs_Monto']
            
            fechas_a_buscar = [
                row_c['Fecha'] + timedelta(days=d) for d in range(-TOLERANCIA_DIAS, TOLERANCIA_DIAS + 1) if d != 0
            ]
            
            df_b_match = df_b_pendiente[
                (df_b_pendiente['Abs_Monto'] == monto_abs) &
                (df_b_pendiente['Fecha'].isin(fechas_a_buscar)) &
                (df_b_pendiente['Conciliado'] == False)
            ].copy()
            
            if not df_b_match.empty:
                best_match_b = df_b_match.iloc[0]
                
                df_c.loc[df_c['ID_Original'] == row_c['ID_Original'], 'Conciliado'] = True
                df_b.loc[df_b['ID_Original'] == best_match_b['ID_Original'], 'Conciliado'] = True
                
                conciliados_tolerancia_list.append({
                    'Estado': f'Conciliado (+/- {TOLERANCIA_DIAS} Días)',
                    'Fecha': best_match_b['Fecha'],
                    'Monto_C': row_c['Monto'],
                    'Monto_B': best_match_b['Monto'],
                    'Concepto_C': row_c['Concepto'],
                    'Concepto_B': best_match_b['Concepto'],
                    f'{ID_COL}_C': row_c[ID_COL],
                    f'{ID_COL}_B': best_match_b[ID_COL]
                })

        df_reporte_tolerancia = pd.DataFrame(conciliados_tolerancia_list, columns=columnas_finales)

    
    # CONSOLIDACIÓN DE LOS PENDIENTES FINALES
    df_c_final = df_c[df_c['Conciliado'] == False].copy()
    df_b_final = df_b[df_b['Conciliado'] == False].copy()

    df_final_pendientes = pd.merge(
        df_c_final, df_b_final, on='Abs_Monto', how='outer', suffixes=('_C', '_B'), indicator=True
    )

    df_reporte_pendientes = formatear_reporte_pendientes(df_final_pendientes)
    
    # CONCATENACIÓN FINAL
    data_frames_to_concat = [df for df in [df_reporte_id, df_reporte_fecha, df_reporte_tolerancia, df_reporte_pendientes] if not df.empty]
    
    df_reporte = pd.concat(data_frames_to_concat, ignore_index=True)
    
    return df_reporte.sort_values(by='Fecha').reset_index(drop=True)

# -------------------------------------------------------------------------------------------------------------------------------------------------
# --- GENERACIÓN DEL ARCHIVO EXCEL (PARA DESCARGA) ---
# -------------------------------------------------------------------------------------------------------------------------------------------------

@st.cache_data
def to_excel_with_summary(df):
    """Genera el archivo Excel completo (2 hojas con formatos) en memoria (BytesIO)."""
    output = io.BytesIO()
    
    writer = pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='dd/mm/yyyy')
    workbook = writer.book
    
    # --- Definición de Formatos ---
    color_conciliado = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
    color_tolerancia = workbook.add_format({'bg_color': '#B7DDF8', 'font_color': '#0B5394'})
    color_contable = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})
    color_banco = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
    formato_numero = workbook.add_format({'num_format': '#,##0.00'}) 
    formato_encabezado_resumen = workbook.add_format({'bold': True, 'align': 'center', 'bg_color': '#D9D9D9', 'border': 1})
    formato_titulo_resumen = workbook.add_format({'bold': True, 'font_size': 14})
    
    
    # 1. HOJA 1: REPORTE DE CONCILIACIÓN Y RESUMEN ESTADÍSTICO
    worksheet = workbook.add_worksheet('Reporte Conciliación')
    writer.sheets['Reporte Conciliación'] = worksheet 
    
    # Preparar y escribir el DataFrame de Resumen (Conteo)
    df_resumen_data = df['Estado'].value_counts().reset_index()
    df_resumen_data.columns = ['Estado', 'Total'] 
    
    worksheet.write('A1', 'RESUMEN DE CONCILIACIÓN', formato_titulo_resumen)
    worksheet.write_row('A2', ['Estado', 'Total'], formato_encabezado_resumen)
    df_resumen_data.to_excel(writer, sheet_name='Reporte Conciliación', startrow=2, startcol=0, index=False, header=False)
    
    # Escribir el DataFrame principal (DETALLE)
    filas_resumen = len(df_resumen_data)
    start_row_detalle = 4 + filas_resumen
    df.to_excel(writer, sheet_name='Reporte Conciliación', startrow=start_row_detalle, index=False)
    
    # Formato condicional para el DETALLE
    rango_datos = f'$A${start_row_detalle + 1}:$H${start_row_detalle + len(df)}'
    
    worksheet.set_column('C:D', 15, formato_numero) 
    worksheet.conditional_format(rango_datos, {'type': 'text', 'criteria': 'containing', 'value': 'Conciliado por ID', 'format': color_conciliado})
    worksheet.conditional_format(rango_datos, {'type': 'text', 'criteria': 'containing', 'value': 'Conciliado por Fecha', 'format': color_conciliado})
    worksheet.conditional_format(rango_datos, {'type': 'text', 'criteria': 'containing', 'value': f'+/- {TOLERANCIA_DIAS} Días', 'format': color_tolerancia}) 
    worksheet.conditional_format(rango_datos, {'type': 'text', 'criteria': 'containing', 'value': 'Solo en Contabilidad', 'format': color_contable})
    worksheet.conditional_format(rango_datos, {'type': 'text', 'criteria': 'containing', 'value': 'Solo en Banco', 'format': color_banco})
    worksheet.set_column('A:A', 35); worksheet.set_column('B:B', 15); worksheet.set_column('E:H', 25) 
    
    
    # 2. HOJA 2: RESUMEN POR CONCEPTO
    df_pendientes = df[df['Estado'].str.contains('Pendiente - Solo en')]
    
    if not df_pendientes.empty:
        df_resumen = df_pendientes[['Estado', 'Concepto_C', 'Concepto_B', 'Monto_C', 'Monto_B']].copy()
        df_resumen['Concepto Final'] = df_resumen['Concepto_C'].fillna(df_resumen['Concepto_B'])
        df_resumen['Monto'] = df_resumen['Monto_C'].fillna(df_resumen['Monto_B'])
        df_agrupado = df_resumen.groupby(['Estado', 'Concepto Final'])['Monto'].sum().reset_index()
        df_agrupado = df_agrupado.rename(columns={'Monto': 'Monto Total Agrupado'})
        
        df_agrupado.to_excel(writer, sheet_name='Resumen Conceptos', index=False)
        
        # Aplicar formato y colores a la HOJA 2
        worksheet_resumen = writer.sheets['Resumen Conceptos']
        worksheet_resumen.set_column('C:C', 15, formato_numero) 
        worksheet_resumen.set_column('A:B', 35)
        rango_resumen = f'$A$2:$C${len(df_agrupado) + 1}'
        worksheet_resumen.conditional_format(rango_resumen, {'type': 'text', 'criteria': 'containing', 'value': 'Solo en Contabilidad', 'format': color_contable})
        worksheet_resumen.conditional_format(rango_resumen, {'type': 'text', 'criteria': 'containing', 'value': 'Solo en Banco', 'format': color_banco})


    writer.close()
    processed_data = output.getvalue()
    return processed_data

# -------------------------------------------------------------------------------------------------------------------------------------------------
# --- ESTRUCTURA PRINCIPAL DE LA APLICACIÓN STREAMLIT ---
# -------------------------------------------------------------------------------------------------------------------------------------------------

# CONFIGURACIÓN DE PÁGINA ANTES DEL LOGIN
st.set_page_config(page_title="Conciliación Bancaria Avanzada", layout="centered")

# VERIFICACIÓN DE ACCESO
if not check_password():
    st.stop() 

# CÓDIGO DE LA APLICACIÓN (SOLO VISIBLE DESPUÉS DEL LOGIN)
st.title("Sistema de Conciliación Bancaria Avanzada 🏦")
st.markdown("Cargue los archivos de Excel para ejecutar la conciliación de **Triple Paso** (ID, Fecha Exacta, y $\pm 3$ Días de Tolerancia).")

# 1. Carga de Archivos
col1, col2 = st.columns(2)

with col1:
    uploaded_contable = st.file_uploader(
        "Archivo de Contabilidad (.xlsx)", 
        type=['xlsx', 'xls'], 
        key="contable_file", 
        help="Debe contener las columnas: Fecha, Debe, Haber, Concepto, Número de operación."
    )

with col2:
    uploaded_bancario = st.file_uploader(
        "Archivo de Resumen Bancario (.xlsx)", 
        type=['xlsx', 'xls'], 
        key="bancario_file", 
        help="Debe contener las columnas: Fecha, Monto, Concepto, Número de operación."
    )

# 2. Botón de Ejecución
if st.button("▶️ EJECUTAR CONCILIACIÓN", type="primary"):
    
    if uploaded_contable and uploaded_bancario:
        
        with st.spinner("Cargando y preparando datos..."):
            df_contable = cargar_datos(uploaded_contable, 'Contable')
            df_bancario = cargar_datos(uploaded_bancario, 'Banco')
        
        if df_contable is not None and df_bancario is not None:
            
            with st.spinner("Ejecutando la lógica de conciliación (Triple Paso)..."):
                df_reporte = conciliar(df_contable, df_bancario)
                
            # Mostrar Resumen en Pantalla
            st.subheader("✅ Conciliación Finalizada")
            st.dataframe(df_reporte['Estado'].value_counts().rename('Total'))
            
            # Generar el archivo Excel en memoria
            excel_data = to_excel_with_summary(df_reporte)
            
            # Botón de Descarga
            st.download_button(
                label="⬇️ Descargar Reporte de Conciliación (Excel)",
                data=excel_data,
                file_name="reporte_conciliacion_final.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="El archivo contendrá dos hojas: Reporte Detallado y Resumen de Conceptos."
            )
            
            st.success("¡Archivo listo para descargar! Revise las dos hojas del Excel.")
            
        else:
            st.error("Hubo un error en la carga o preparación de los archivos. Revise los mensajes de error arriba.")

    else:
        st.warning("Por favor, sube ambos archivos para iniciar la conciliación.")
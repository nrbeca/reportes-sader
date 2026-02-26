# ============================================================================
# PROCESADOR DE ARCHIVOS MAP - BASADO EN SCRIPT COLAB ORIGINAL
# ============================================================================

import pandas as pd
import numpy as np
from datetime import date
from config import (
    MONTH_NAMES, round_like_excel, detectar_fecha_archivo,
    get_config_by_year, numero_a_letras_mx
)


def sum_columns(df, prefix, months_to_use):
    """Suma las columnas de un prefijo para los meses especificados"""
    cols = [f'{prefix}_{month}' for month in months_to_use if f'{prefix}_{month}' in df.columns]
    if not cols:
        return pd.Series([0] * len(df))
    result = df[cols].fillna(0).sum(axis=1)
    return result.apply(lambda x: round_like_excel(x, 2))


def crear_pivot_suma(df, filtro_func, descripcion=""):
    """Crea una suma de Original, ModificadoAnualNeto, ModificadoPeriodoNeto, Ejercido"""
    filtered = df[filtro_func(df)]
    if len(filtered) == 0:
        return {
            'Original': 0,
            'ModificadoAnualNeto': 0,
            'ModificadoPeriodoNeto': 0,
            'Ejercido': 0
        }
    return {
        'Original': round(filtered['Original'].sum(), 2),
        'ModificadoAnualNeto': round(filtered['ModificadoAnualNeto'].sum(), 2),
        'ModificadoPeriodoNeto': round(filtered['ModificadoPeriodoNeto'].sum(), 2),
        'Ejercido': round(filtered['Ejercido'].sum(), 2)
    }


def calcular_congelado_programa(df, programa):
    """Calcula el congelado anual de un programa específico"""
    df_programa = df[df['Pp'] == programa]
    if len(df_programa) == 0:
        return 0
    return round_like_excel(df_programa['CongeladoAnual'].sum(), 2)


def procesar_map(df, filename):
    """Procesa un archivo MAP y genera el resumen presupuestario"""
    
    # Detectar fecha del archivo
    fecha_archivo, mes_archivo, año_archivo = detectar_fecha_archivo(filename)
    
    # Obtener configuración según el año
    config = get_config_by_year(año_archivo)
    
    # Meses para columnas
    month_names = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    months_up_to_current = month_names[:mes_archivo]
    
    # Obtener configuración de programas
    PROGRAMAS_ESPECIFICOS = config['programas_especificos']
    PROGRAMAS_NOMBRES = config['programas_nombres']
    NOMBRES_ESPECIALES = config['nombres_especiales']
    FUSION_PROGRAMAS = config.get('fusion_programas', {})
    
    # =========================================================================
    # CALCULAR COLUMNAS (igual que script original)
    # =========================================================================
    
    # Mapeo de URs
    ur_map = {
        121: 260, 122: 261, 123: 262, 124: 263, 125: 264, 126: 265, 127: 266, 128: 267,
        129: 268, 130: 269, 131: 270, 132: 271, 133: 272, 134: 273, 135: 274, 136: 275,
        137: 276, 138: 277, 139: 278, 140: 279, 141: 280, 142: 281, 143: 282, 144: 283,
        145: 284, 146: 285, 147: 286, 148: 287, 149: 288, 150: 289, 151: 290, 152: 291,
        153: 292, 108: 810, 215: 220, 300: 225, 310: 226, 700: 227, 600: 230, 612: 231,
        312: 232, 315: 233, 400: 235, 311: 237, 314: 245, 113: 250
    }
    
    # NuevaUR
    df['NuevaUR'] = df['UNIDAD'].apply(
        lambda x: 811 if x == 'G00' else ur_map.get(int(x) if str(x).isdigit() else 0, int(x) if str(x).isdigit() else 0)
    )
    
    # Pp (Programa Presupuestario)
    df['Pp_Original'] = df['IDEN_PROY'].astype(str) + df['PROYECTO'].astype(str).str.zfill(3)
    
    # Aplicar fusión de programas
    def mapear_programa(pp):
        if pp in FUSION_PROGRAMAS:
            return FUSION_PROGRAMAS[pp]
        return pp
    
    df['Pp'] = df['Pp_Original'].apply(mapear_programa)
    
    # Capítulo
    df['PARTIDA'] = pd.to_numeric(df['PARTIDA'], errors='coerce').fillna(0).astype(int)
    df['Capitulo'] = (df['PARTIDA'] // 10000) * 1000
    
    # Llave
    df['Llave'] = df['NuevaUR'].astype(str) + df['PARTIDA'].astype(str) + df['Pp'].astype(str)
    
    # Redondear valores base
    for prefix in ['ORI', 'AMP', 'RED', 'MOD', 'CONG', 'DESCONG', 'EJE']:
        for month in month_names:
            col = f'{prefix}_{month}'
            if col in df.columns:
                df[col] = df[col].fillna(0).apply(lambda x: round_like_excel(x, 2))
    
    # =========================================================================
    # CALCULAR TOTALES
    # =========================================================================
    
    año_actual = date.today().year
    es_cierre_año_anterior = (mes_archivo in [1, 2]) and (año_archivo < año_actual)
    
    # Original
    df['Original'] = sum_columns(df, 'ORI', month_names)
    df['OriginalPeriodo'] = sum_columns(df, 'ORI', months_up_to_current)
    
    # Modificado Anual Bruto
    df['ModificadoAnualBruto'] = sum_columns(df, 'MOD', month_names)
    
    # Modificado Periodo Bruto
    if es_cierre_año_anterior:
        df['ModificadoPeriodoBruto'] = sum_columns(df, 'MOD', month_names)
    else:
        df['ModificadoPeriodoBruto'] = sum_columns(df, 'MOD', months_up_to_current)
    
    # Congelados
    cong_anual = sum_columns(df, 'CONG', month_names)
    descong_anual = sum_columns(df, 'DESCONG', month_names)
    
    if es_cierre_año_anterior:
        cong_periodo = sum_columns(df, 'CONG', month_names)
        descong_periodo = sum_columns(df, 'DESCONG', month_names)
    else:
        cong_periodo = sum_columns(df, 'CONG', months_up_to_current)
        descong_periodo = sum_columns(df, 'DESCONG', months_up_to_current)
    
    df['CongeladoAnual'] = (cong_anual - descong_anual).apply(lambda x: round_like_excel(x, 2))
    df['CongeladoPeriodo'] = (cong_periodo - descong_periodo).apply(lambda x: round_like_excel(x, 2))
    
    # Modificado Neto
    mod_anual_sum = sum_columns(df, 'MOD', month_names)
    df['ModificadoAnualNeto'] = (mod_anual_sum - df['CongeladoAnual']).apply(lambda x: round_like_excel(x, 2))
    
    if es_cierre_año_anterior:
        df['ModificadoPeriodoNeto'] = df['ModificadoAnualNeto'].copy()
    else:
        mod_periodo_sum = sum_columns(df, 'MOD', months_up_to_current)
        df['ModificadoPeriodoNeto'] = (mod_periodo_sum - df['CongeladoPeriodo']).apply(lambda x: round_like_excel(x, 2))
    
    # Ejercido
    df['Ejercido'] = sum_columns(df, 'EJE', month_names)
    
    # Disponibles
    df['DisponibleAnualNeto'] = (df['ModificadoAnualNeto'] - df['Ejercido']).apply(lambda x: round_like_excel(x, 2))
    df['DisponiblePeriodoNeto'] = (df['ModificadoPeriodoNeto'] - df['Ejercido']).apply(lambda x: round_like_excel(x, 2))
    
    # =========================================================================
    # CONGELADOS POR PROGRAMA
    # =========================================================================
    programas_con_congelados = ['S263', 'S293', 'S304']
    congelados_valores = {}
    congelados_textos = {}
    
    for prog in programas_con_congelados:
        congelados_valores[prog] = calcular_congelado_programa(df, prog)
        congelados_textos[prog] = numero_a_letras_mx(congelados_valores[prog])
    
    # =========================================================================
    # CREAR TABLAS DINÁMICAS (PIVOTS) - IGUAL QUE SCRIPT ORIGINAL
    # =========================================================================
    
    # Cap 1000 (Servicios Personales) - EXCLUYENDO programas específicos
    pivot_cap1000 = crear_pivot_suma(
        df,
        lambda df: (df['Capitulo'] == 1000) & (~df['Pp'].isin(PROGRAMAS_ESPECIFICOS)),
        "Cap 1000"
    )
    
    # Cap 2000 + 3000 (Gasto Corriente) - EXCLUYENDO programas específicos
    pivot_cap2000_3000 = crear_pivot_suma(
        df,
        lambda df: (df['Capitulo'].isin([2000, 3000])) & (~df['Pp'].isin(PROGRAMAS_ESPECIFICOS)),
        "Cap 2000+3000"
    )
    
    # Programas específicos (subsidios)
    pivot_programas = {}
    for prog in PROGRAMAS_ESPECIFICOS:
        pivot_programas[prog] = crear_pivot_suma(df, lambda df, p=prog: df['Pp'] == p, f"Programa {prog}")
    
    # Cap 4000 (Otros programas) - EXCLUYENDO programas específicos
    pivot_cap4000 = crear_pivot_suma(
        df,
        lambda df: (df['Capitulo'] == 4000) & (~df['Pp'].isin(PROGRAMAS_ESPECIFICOS)),
        "Cap 4000"
    )
    
    # Cap 5000 + 7000 (Bienes Muebles) - EXCLUYENDO programas específicos
    pivot_cap5000_7000 = crear_pivot_suma(
        df,
        lambda df: (df['Capitulo'].isin([5000, 7000])) & (~df['Pp'].isin(PROGRAMAS_ESPECIFICOS)),
        "Cap 5000+7000"
    )
    
    # =========================================================================
    # CALCULAR SUBTOTALES Y TOTALES
    # =========================================================================
    
    # Subtotal subsidios
    subtotal_subsidios = {
        'Original': sum(pivot_programas[p]['Original'] for p in PROGRAMAS_ESPECIFICOS),
        'ModificadoAnualNeto': sum(pivot_programas[p]['ModificadoAnualNeto'] for p in PROGRAMAS_ESPECIFICOS),
        'ModificadoPeriodoNeto': sum(pivot_programas[p]['ModificadoPeriodoNeto'] for p in PROGRAMAS_ESPECIFICOS),
        'Ejercido': sum(pivot_programas[p]['Ejercido'] for p in PROGRAMAS_ESPECIFICOS),
    }
    
    # Totales
    totales = {
        'Original': (pivot_cap1000['Original'] + pivot_cap2000_3000['Original'] +
                     subtotal_subsidios['Original'] +
                     pivot_cap4000['Original'] + pivot_cap5000_7000['Original']),
        'ModificadoAnualNeto': (pivot_cap1000['ModificadoAnualNeto'] + pivot_cap2000_3000['ModificadoAnualNeto'] +
                                subtotal_subsidios['ModificadoAnualNeto'] +
                                pivot_cap4000['ModificadoAnualNeto'] + pivot_cap5000_7000['ModificadoAnualNeto']),
        'ModificadoPeriodoNeto': (pivot_cap1000['ModificadoPeriodoNeto'] + pivot_cap2000_3000['ModificadoPeriodoNeto'] +
                                  subtotal_subsidios['ModificadoPeriodoNeto'] +
                                  pivot_cap4000['ModificadoPeriodoNeto'] + pivot_cap5000_7000['ModificadoPeriodoNeto']),
        'Ejercido': (pivot_cap1000['Ejercido'] + pivot_cap2000_3000['Ejercido'] +
                     subtotal_subsidios['Ejercido'] +
                     pivot_cap4000['Ejercido'] + pivot_cap5000_7000['Ejercido']),
    }
    
    # =========================================================================
    # CATEGORÍAS PARA COMPATIBILIDAD
    # =========================================================================
    categorias = {
        'servicios_personales': pivot_cap1000,
        'gasto_corriente': pivot_cap2000_3000,
        'subsidios': subtotal_subsidios,
        'otros_programas': pivot_cap4000,
        'bienes_muebles': pivot_cap5000_7000,
    }
    
    # =========================================================================
    # DATOS PARA DASHBOARD POR UR (filtrado diferente)
    # =========================================================================
    PARTIDAS_EXCLUIR = [39801, 39810]
    df_dashboard = df[(df['Capitulo'] != 1000) & (~df['PARTIDA'].isin(PARTIDAS_EXCLUIR))].copy()
    
    resultados_por_ur = {}
    capitulos_por_ur = {}
    partidas_por_ur = {}
    
    for ur in df['UNIDAD'].unique():
        ur_str = str(ur).strip()
        df_ur = df_dashboard[df_dashboard['UNIDAD'].astype(str).str.strip() == ur_str]
        
        if len(df_ur) == 0:
            continue
        
        original = round_like_excel(df_ur['Original'].sum(), 2)
        mod_anual = round_like_excel(df_ur['ModificadoAnualNeto'].sum(), 2)
        mod_periodo = round_like_excel(df_ur['ModificadoPeriodoNeto'].sum(), 2)
        ejercido = round_like_excel(df_ur['Ejercido'].sum(), 2)
        
        resultados_por_ur[ur_str] = {
            'Original': original,
            'Modificado_anual': mod_anual,
            'Modificado_periodo': mod_periodo,
            'Ejercido': ejercido,
            'Disponible_anual': round_like_excel(mod_anual - ejercido, 2),
            'Disponible_periodo': round_like_excel(mod_periodo - ejercido, 2),
            'Congelado_anual': 0,
            'Congelado_periodo': 0,
            'Pct_avance_anual': ejercido / mod_anual if mod_anual > 0 else 0,
            'Pct_avance_periodo': ejercido / mod_periodo if mod_periodo > 0 else 0,
        }
        
        # Por capítulo (2, 3, 4)
        caps = {}
        for cap in [2, 3, 4]:
            df_cap = df_ur[df_ur['Capitulo'] == cap * 1000]
            caps[str(cap)] = {
                'Original': round_like_excel(df_cap['Original'].sum(), 2),
                'Modificado_anual': round_like_excel(df_cap['ModificadoAnualNeto'].sum(), 2),
                'Modificado_periodo': round_like_excel(df_cap['ModificadoPeriodoNeto'].sum(), 2),
                'Ejercido': round_like_excel(df_cap['Ejercido'].sum(), 2),
            }
        capitulos_por_ur[ur_str] = caps
        
        # Top partidas con mayor disponible
        df_part = df_ur.groupby(['PARTIDA', 'Pp']).agg({
            'Original': 'sum',
            'ModificadoPeriodoNeto': 'sum',
            'Ejercido': 'sum'
        }).reset_index()
        df_part['Disponible'] = df_part['ModificadoPeriodoNeto'] - df_part['Ejercido']
        df_part = df_part[df_part['Disponible'] > 0].sort_values('Disponible', ascending=False).head(5)
        
        partidas_list = []
        for _, row in df_part.iterrows():
            partidas_list.append({
                'Partida': int(row['PARTIDA']),
                'Programa': row['Pp'],
                'Denom_Programa': PROGRAMAS_NOMBRES.get(row['Pp'], ''),
                'Disponible': round_like_excel(row['Disponible'], 2),
            })
        partidas_por_ur[ur_str] = partidas_list
    
    # =========================================================================
    # RETORNAR RESULTADOS
    # =========================================================================
    return {
        'totales': totales,
        'categorias': categorias,
        'programas': pivot_programas,
        'congelados': {
            'valores': congelados_valores,
            'textos': congelados_textos,
        },
        'resultados_por_ur': resultados_por_ur,
        'capitulos_por_ur': capitulos_por_ur,
        'partidas_por_ur': partidas_por_ur,
        'metadata': {
            'fecha_archivo': fecha_archivo,
            'mes': mes_archivo,
            'año': año_archivo,
            'registros': len(df),
            'config': config,
            'es_cierre_año_anterior': es_cierre_año_anterior,
        },
        'df_procesado': df,
    }

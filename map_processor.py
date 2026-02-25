# ============================================================================
# PROCESADOR DE ARCHIVOS MAP
# ============================================================================

import pandas as pd
import numpy as np
from datetime import date
from config import (
    MONTH_NAMES, round_like_excel, detectar_fecha_archivo,
    get_config_by_year, numero_a_letras_mx
)


def procesar_map(df, filename):
    """Procesa un archivo MAP y genera el resumen presupuestario"""
    
    # Detectar fecha del archivo
    fecha_archivo, mes_archivo, año_archivo = detectar_fecha_archivo(filename)
    
    # Obtener configuración según el año
    config = get_config_by_year(año_archivo)
    
    # Meses para columnas
    meses = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    
    # Columnas por tipo
    cols_ori = [f'ORI_{m}' for m in meses]
    cols_mod = [f'MOD_{m}' for m in meses]
    cols_mod_periodo = [f'MOD_{m}' for m in meses[:mes_archivo]]
    cols_eje = [f'EJE_{m}' for m in meses]
    cols_cong = [f'CONG_{m}' for m in meses]
    cols_cong_periodo = [f'CONG_{m}' for m in meses[:mes_archivo]]
    
    # Calcular totales por fila
    df['ORIGINAL'] = df[cols_ori].sum(axis=1)
    df['MOD_ANUAL'] = df[cols_mod].sum(axis=1)
    df['MOD_PERIODO'] = df[cols_mod_periodo].sum(axis=1)
    df['EJERCIDO'] = df[cols_eje].sum(axis=1)
    df['CONG_ANUAL'] = df[cols_cong].sum(axis=1) if all(c in df.columns for c in cols_cong) else 0
    df['CONG_PERIODO'] = df[cols_cong_periodo].sum(axis=1) if all(c in df.columns for c in cols_cong_periodo) else 0
    
    # Extraer capitulo de PARTIDA
    df['CAPITULO'] = df['PARTIDA'].astype(str).str[0].astype(int)
    
    # =========================================================================
    # FILTROS PARA DASHBOARD PRESUPUESTO
    # - Excluir Capítulo 1 (Servicios Personales)
    # - Excluir partida 39801
    # =========================================================================
    PARTIDAS_EXCLUIR = [39801, 39810]  # Partidas a excluir del dashboard
    
    df_dashboard = df[(df['CAPITULO'] != 1) & (~df['PARTIDA'].isin(PARTIDAS_EXCLUIR))].copy()
    
    # =========================================================================
    # CALCULOS POR UR PARA DASHBOARD
    # =========================================================================
    
    resultados_por_ur = {}
    capitulos_por_ur = {}
    partidas_por_ur = {}
    
    for ur in df['UNIDAD'].unique():
        ur_str = str(ur).strip()
        
        # Datos filtrados para dashboard (sin cap 1, sin 39801)
        df_ur = df_dashboard[df_dashboard['UNIDAD'].astype(str).str.strip() == ur_str]
        
        if len(df_ur) == 0:
            continue
        
        # KPIs principales
        original = round_like_excel(df_ur['ORIGINAL'].sum(), 2)
        mod_anual = round_like_excel(df_ur['MOD_ANUAL'].sum(), 2)
        mod_periodo = round_like_excel(df_ur['MOD_PERIODO'].sum(), 2)
        ejercido = round_like_excel(df_ur['EJERCIDO'].sum(), 2)
        cong_anual = round_like_excel(df_ur['CONG_ANUAL'].sum(), 2)
        cong_periodo = round_like_excel(df_ur['CONG_PERIODO'].sum(), 2)
        
        disp_anual = round_like_excel(mod_anual - ejercido, 2)
        disp_periodo = round_like_excel(mod_periodo - ejercido, 2)
        
        resultados_por_ur[ur_str] = {
            'Original': original,
            'Modificado_anual': mod_anual,
            'Modificado_periodo': mod_periodo,
            'Ejercido': ejercido,
            'Disponible_anual': disp_anual,
            'Disponible_periodo': disp_periodo,
            'Congelado_anual': cong_anual,
            'Congelado_periodo': cong_periodo,
            'Pct_avance_anual': ejercido / mod_anual if mod_anual > 0 else 0,
            'Pct_avance_periodo': ejercido / mod_periodo if mod_periodo > 0 else 0,
        }
        
        # Por capítulo
        caps = {}
        for cap in [2, 3, 4]:
            df_cap = df_ur[df_ur['CAPITULO'] == cap]
            caps[str(cap)] = {
                'Original': round_like_excel(df_cap['ORIGINAL'].sum(), 2),
                'Modificado_anual': round_like_excel(df_cap['MOD_ANUAL'].sum(), 2),
                'Modificado_periodo': round_like_excel(df_cap['MOD_PERIODO'].sum(), 2),
                'Ejercido': round_like_excel(df_cap['EJERCIDO'].sum(), 2),
            }
        capitulos_por_ur[ur_str] = caps
        
        # Top partidas con mayor disponible
        df_part = df_ur.groupby(['PARTIDA', 'PROGRAMA']).agg({
            'ORIGINAL': 'sum',
            'MOD_ANUAL': 'sum',
            'MOD_PERIODO': 'sum',
            'EJERCIDO': 'sum'
        }).reset_index()
        df_part['Disponible'] = df_part['MOD_PERIODO'] - df_part['EJERCIDO']
        df_part = df_part[df_part['Disponible'] > 0].sort_values('Disponible', ascending=False).head(5)
        
        partidas_list = []
        for _, row in df_part.iterrows():
            partidas_list.append({
                'Partida': int(row['PARTIDA']),
                'Programa': row['PROGRAMA'],
                'Denom_Programa': config['programas_nombres'].get(row['PROGRAMA'], ''),
                'Disponible': round_like_excel(row['Disponible'], 2),
            })
        partidas_por_ur[ur_str] = partidas_list
    
    # =========================================================================
    # CALCULOS GLOBALES (para el reporte general MAP - incluye todo)
    # =========================================================================
    
    # Totales generales (sin filtrar, para compatibilidad con reporte MAP original)
    totales = {
        'Original': round_like_excel(df['ORIGINAL'].sum(), 2),
        'ModificadoAnualNeto': round_like_excel(df['MOD_ANUAL'].sum(), 2),
        'ModificadoPeriodoNeto': round_like_excel(df['MOD_PERIODO'].sum(), 2),
        'Ejercido': round_like_excel(df['EJERCIDO'].sum(), 2),
    }
    
    # Por categoría (para reporte MAP original)
    categorias = {}
    
    # Servicios personales = Cap 1
    df_sp = df[df['CAPITULO'] == 1]
    categorias['servicios_personales'] = {
        'Original': round_like_excel(df_sp['ORIGINAL'].sum(), 2),
        'ModificadoAnualNeto': round_like_excel(df_sp['MOD_ANUAL'].sum(), 2),
        'ModificadoPeriodoNeto': round_like_excel(df_sp['MOD_PERIODO'].sum(), 2),
        'Ejercido': round_like_excel(df_sp['EJERCIDO'].sum(), 2),
    }
    
    # Gasto corriente = Cap 2 y 3
    df_gc = df[df['CAPITULO'].isin([2, 3])]
    categorias['gasto_corriente'] = {
        'Original': round_like_excel(df_gc['ORIGINAL'].sum(), 2),
        'ModificadoAnualNeto': round_like_excel(df_gc['MOD_ANUAL'].sum(), 2),
        'ModificadoPeriodoNeto': round_like_excel(df_gc['MOD_PERIODO'].sum(), 2),
        'Ejercido': round_like_excel(df_gc['EJERCIDO'].sum(), 2),
    }
    
    # Subsidios = Cap 4
    df_sub = df[df['CAPITULO'] == 4]
    categorias['subsidios'] = {
        'Original': round_like_excel(df_sub['ORIGINAL'].sum(), 2),
        'ModificadoAnualNeto': round_like_excel(df_sub['MOD_ANUAL'].sum(), 2),
        'ModificadoPeriodoNeto': round_like_excel(df_sub['MOD_PERIODO'].sum(), 2),
        'Ejercido': round_like_excel(df_sub['EJERCIDO'].sum(), 2),
    }
    
    # Otros = Cap 5, 6, 7
    df_otros = df[df['CAPITULO'].isin([5, 6, 7])]
    categorias['otros_programas'] = {
        'Original': round_like_excel(df_otros['ORIGINAL'].sum(), 2),
        'ModificadoAnualNeto': round_like_excel(df_otros['MOD_ANUAL'].sum(), 2),
        'ModificadoPeriodoNeto': round_like_excel(df_otros['MOD_PERIODO'].sum(), 2),
        'Ejercido': round_like_excel(df_otros['EJERCIDO'].sum(), 2),
    }
    
    # Bienes muebles = Cap 5
    df_bm = df[df['CAPITULO'] == 5]
    categorias['bienes_muebles'] = {
        'Original': round_like_excel(df_bm['ORIGINAL'].sum(), 2),
        'ModificadoAnualNeto': round_like_excel(df_bm['MOD_ANUAL'].sum(), 2),
        'ModificadoPeriodoNeto': round_like_excel(df_bm['MOD_PERIODO'].sum(), 2),
        'Ejercido': round_like_excel(df_bm['EJERCIDO'].sum(), 2),
    }
    
    # Por programa
    programas = {}
    for prog in df['PROGRAMA'].unique():
        df_prog = df[df['PROGRAMA'] == prog]
        programas[prog] = {
            'Original': round_like_excel(df_prog['ORIGINAL'].sum(), 2),
            'ModificadoAnualNeto': round_like_excel(df_prog['MOD_ANUAL'].sum(), 2),
            'ModificadoPeriodoNeto': round_like_excel(df_prog['MOD_PERIODO'].sum(), 2),
            'Ejercido': round_like_excel(df_prog['EJERCIDO'].sum(), 2),
        }
    
    # Calcular congelados totales
    congelado_anual_total = df['CONG_ANUAL'].sum() if 'CONG_ANUAL' in df.columns else 0
    congelado_periodo_total = df['CONG_PERIODO'].sum() if 'CONG_PERIODO' in df.columns else 0

    return {
        'congelados': {
            'anual': congelado_anual_total,
            'periodo': congelado_periodo_total,
            'texto_anual': numero_a_letras_mx(congelado_anual_total),
            'texto_periodo': numero_a_letras_mx(congelado_periodo_total),
        },
        'totales': totales,
        'categorias': categorias,
        'programas': programas,
        'resultados_por_ur': resultados_por_ur,
        'capitulos_por_ur': capitulos_por_ur,
        'partidas_por_ur': partidas_por_ur,
        'metadata': {
            'fecha_archivo': fecha_archivo,
            'mes': mes_archivo,
            'año': año_archivo,
            'registros': len(df),
            'config': config,
        },
        'df_procesado': df,
    }

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import io
import numpy as np
import sqlite3 as sql
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import os
from pathlib import Path
import logging
# Configuração robusta do logger para evitar múltiplos handlers e garantir logs no console

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Garante que o diretório 'exportado' existe
os.makedirs('exportado', exist_ok=True)

st.set_page_config(page_title="Demanda Coincidente", page_icon='icone', layout='wide')

st.header('Demanda Coincidente')
st.markdown("Assessoria de Planejamento e Orçamento")
        
# seleção única de data e hora (escopo global, antes do botão Calcular)
col1, col2 = st.columns(2)
with col1:
    data = st.date_input("Selecione a data", value=pd.Timestamp('2024-01-01'))
with col2:
    hora = st.selectbox("Selecione a hora", options=range(24), format_func=lambda x: f"{x:02d}:00")
data_hora_coincidente = pd.Timestamp.combine(data, pd.Timestamp(f"{hora:02d}:00:00").time())

# Função utilitária para buscar os códigos dos equipamentos
def importa_codigo_equipamentos():
    try:
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_path = os.path.join(root_dir, 'Cadastro', 'Dados.db')
        with sql.connect(db_path) as conn:
            query = "SELECT DISTINCT codigo_trafo_alimentador FROM dados_tecnicos"
            codigos = pd.read_sql_query(query, conn)['codigo_trafo_alimentador'].dropna().unique()
        return codigos
    except Exception as e:
        st.error(f"Erro ao buscar códigos do banco: {str(e)}")
        return []

codigos = importa_codigo_equipamentos()
# Inicializa o session_state se necessário
if "selecao_codigos" not in st.session_state:
    st.session_state["selecao_codigos"] = []

# Botão para selecionar todos
if st.button("Selecionar Todos"):
    st.session_state["selecao_codigos"] = list(codigos)
    st.rerun()

# Multiselect controlado apenas pelo session_state
selecao = st.multiselect("Selecione os Cód. do Equipamento:", codigos, key="selecao_codigos")


# Processamento principal ao clicar no botão
if st.button("Calcular"):
    # Corrige o caminho para a raiz do projeto
    base_path = Path(__file__).resolve().parent.parent
    db_path = base_path / "Cadastro" / "Dados.db"
    db_path_medicoes = base_path / "DataBase" / "Medicoes.db"
    # Checagem de existência dos bancos
    if not db_path.exists():
        st.error(f"Arquivo de banco de dados não encontrado: {db_path}")
    elif not db_path_medicoes.exists():
        st.error(f"Arquivo de banco de dados não encontrado: {db_path_medicoes}")
    else:
        # Carregar dados técnicos para potência instalada
        with sql.connect(db_path) as conexao:
            df_dados_tecnicos = pd.read_sql_query("SELECT * FROM dados_tecnicos", conexao)
        todos_resultados = []
        for equipamento in selecao:
            try:
                EAE = equipamento + '-EAE'
                EAR = equipamento + '-EAR'
                ERE = equipamento + '-ERE'
                ERR = equipamento + '-ERR'
                with sql.connect(db_path) as conexao:
                    query_EAE = "SELECT descricao FROM medidores_hemera WHERE Codigo = ?"
                    descricao = pd.read_sql_query(query_EAE, conexao, params=(EAE,)).iloc[0, 0]
                    query_EAR = "SELECT descricao FROM medidores_hemera WHERE Codigo = ?"
                    descricao_saida = pd.read_sql_query(query_EAR, conexao, params=(EAR,)).iloc[0, 0]
                    query_ERE = "SELECT descricao FROM medidores_hemera WHERE Codigo = ?"
                    descricao_Q = pd.read_sql_query(query_ERE, conexao, params=(ERE,)).iloc[0, 0]
                    query_ERR = "SELECT descricao FROM medidores_hemera WHERE Codigo = ?"
                    descricao_saida_Q = pd.read_sql_query(query_ERR, conexao, params=(ERR,)).iloc[0, 0]

                # Consulta filtrada por data/hora
                with sql.connect(str(db_path_medicoes)) as conexao_medicoes:
                    cursor = conexao_medicoes.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tabelas = [row[0] for row in cursor.fetchall()]
                    resultados = []
                for tabela in tabelas:
                    try:
                        # (Removido diagnóstico de datas/horas disponíveis)
                        # Só consulta a tabela se ela contém todas as colunas necessárias
                        colunas_necessarias = [descricao, descricao_saida, descricao_Q, descricao_saida_Q]
                        cursor.execute(f'PRAGMA table_info({tabela})')
                        colunas_tabela = [row[1] for row in cursor.fetchall()]
                        if all(col in colunas_tabela for col in colunas_necessarias):
                            # Verifica se o equipamento realmente tem dados na tabela
                            query_existe = f"SELECT COUNT(1) as total FROM {tabela} WHERE [{descricao}] IS NOT NULL"
                            existe_df = pd.read_sql_query(query_existe, conexao_medicoes)
                            if existe_df['total'].iloc[0] > 0:
                                query = f"""
                                    SELECT 
                                        [Data_Hora] AS Data_Hora, 
                                        [{descricao}] AS descricao,
                                        [{descricao_saida}] AS descricao_saida,
                                        [{descricao_Q}] AS descricao_Q,
                                        [{descricao_saida_Q}] AS descricao_saida_Q
                                    FROM {tabela}
                                    WHERE [Data_Hora] = ?
                                """
                                # Garante que o parâmetro seja string no formato do banco
                                data_hora_str = data_hora_coincidente.strftime('%Y-%m-%d %H:%M:%S')
                                df = pd.read_sql_query(query, conexao_medicoes, params=(data_hora_str,))
                                if not df.empty:
                                    df["Data_Hora"] = pd.to_datetime(df["Data_Hora"], errors='coerce')
                                    df["tabela_origem"] = tabela
                                    resultados.append(df)
                                # else: (Removido log de ausência de dados)
                            # else: (Removido log de tabela ignorada por falta de dados)
                        # else: (Removido log de tabela ignorada por falta de colunas)
                    except Exception as e:
                        pass
                if not resultados:
                    st.info(f"Nenhum dado encontrado para o equipamento '{equipamento}' no horário selecionado.")
                else:
                    base = pd.concat(resultados, ignore_index=True)
                    base['P'] = (base['descricao'] - base['descricao_saida']).fillna(0)
                    base['Q'] = (base['descricao_Q'] - base['descricao_saida_Q']).fillna(0)
                    base['S'] = np.sqrt((base['P']**2) + (base['Q']**2))
                    base['S'] = base['S'].replace(0, np.nan)  # evita divisão por zero
                    base['fp'] = base['P'] / base['S']
                    base['fp'] = base['fp'].replace([np.inf, -np.inf], np.nan).fillna(0)
                    # Adiciona o resultado do horário selecionado
                    if data_hora_coincidente in base['Data_Hora'].values:
                        linha = base[base['Data_Hora'] == data_hora_coincidente]
                        todos_resultados.append({
                            'Equipamento': equipamento,
                            'Data/Hora': linha['Data_Hora'].iloc[0] if not linha.empty else data_hora_str,
                            'Potência Ativa (kW)': linha['P'].max(),
                            'Potência Reativa (kvar)': linha['Q'].max(),
                            'Potência Aparente (kVA)': linha['S'].max(),
                            'FP': linha['fp'].max(),
                        })
            except Exception as e:
                st.error(f"Erro ao processar equipamento {equipamento}: {str(e)}")
                continue

        # Exibir todos os resultados em uma única tabela
        if todos_resultados:
            resultados_df = pd.DataFrame(todos_resultados)
            st.table(resultados_df)
            # Exportação para Excel
            output = io.BytesIO()
            resultados_df.to_excel(output, index=False, sheet_name='Resultados')

            # Salvar arquivo no diretório exportado
            export_path = os.path.join('exportado', f"demanda_coincidente_{data_hora_coincidente.strftime('%Y%m%d_%H%M')}.xlsx")
            resultados_df.to_excel(export_path, index=False, sheet_name='Resultados')

            # Botão de download
            st.download_button(
                label="Exportar Dados (Excel)",
                data=output.getvalue(),
                file_name=f"demanda_coincidente_{data_hora_coincidente.strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("Nenhum resultado foi calculado.")

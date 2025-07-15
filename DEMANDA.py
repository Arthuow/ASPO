#Projeto Energisa de Leitura de dados de Demanda
import os
import warnings
import pandas as pd
warnings.filterwarnings("ignore")
import streamlit as st
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import io
from pathlib import Path
import sqlite3 as sql
import logging


# Configurar logger global
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configurar diretório de logs
try:
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'exportado')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'demanda.log')
    try:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)
    except Exception as e:
        print(f"Warning: Could not set up file logging: {str(e)}")
except Exception as e:
    print(f"Error setting up logging: {str(e)}")


class pagina_principal:
    # Configuração da página Streamlit
    st.set_page_config(page_title="Energisa Mato Grosso", page_icon='icone', layout='wide')
    # Interface do usuário
    st.header('Energisa Mato Grosso - ASPO')
    st.markdown("Assessoria de Planejamento e Orçamento")

    @staticmethod
    @st.cache_data
    def importa_base():
        logger.info("Importando toda a base de dados do SQLite")
        try:
            base_path = Path(__file__).resolve().parent
            db_path = base_path / "DataBase" / "Medicoes.db"
            conn = sql.connect(str(db_path))
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tabelas = [row[0] for row in cursor.fetchall()]

            base_completa = {}
            for tabela in tabelas:
                query = f"SELECT * FROM {tabela}"
                base_completa[tabela] = pd.read_sql_query(query, conn)

            logger.info("Base de dados completa importada com sucesso")
            return base_completa

        except Exception as e:
            logger.error(f"Erro ao importar toda a base de dados: {str(e)}")
            st.error(f"Erro ao importar toda a base de dados: {str(e)}")
            return None

        finally:
            if 'conn' in locals():
                conn.close()

    @st.cache_data
    def importar_base_equipamentos():
        try:
            base_path = Path(__file__).resolve().parent
            db_path = base_path / "Cadastro" / "Dados.db"
            logger.info(f"Caminho do banco para importar equipamentos: {db_path}")
            logger.info(f"Arquivo existe? {db_path.exists()}")

            with sql.connect(str(db_path)) as conexao:
                logger.info("Conectado ao Banco: Dados.db")
                query = "SELECT codigo_trafo_alimentador FROM dados_tecnicos"
                df_equipamentos = pd.read_sql_query(query, conexao)
                logger.info("Importação dos códigos dos equipamentos concluída")
                return df_equipamentos
        except Exception as e:
            logger.error(f"Erro ao importar os códigos dos equipamentos: {str(e)}")
            st.error(f"Erro ao importar os códigos dos equipamentos: {str(e)}")
            return None
    # Importar base de equipamentos
    df_equipamentos = importar_base_equipamentos()
    # Importa dados de demanda máxima
    def carregar_dados_demanda_maxima():
        try:
            with sql.connect("DataBase/Relatorios.db") as conexao:
                query = "SELECT * FROM demanda_maxima_mensal"
                df_maxima = pd.read_sql_query(query, conexao)
                logger.info("Dados de demanda máxima carregados com sucesso")
                return df_maxima
        except Exception as e:
            st.error("Erro ao carregar dados de demanda máxima.")
            return pd.DataFrame()
    # Carregar dados de demanda máxima
    df_maxima = carregar_dados_demanda_maxima()
    
    # Carrega dados dos medidores Hemera
    def carregar_medidores_hemera():
        try:
            base_path = Path(__file__).resolve().parent
            db_path = base_path / "Cadastro" / "Dados.db"
            with sql.connect(db_path) as conexao:
                query = "SELECT * FROM medidores_hemera"
                df_atributos = pd.read_sql_query(query, conexao)
                logger.info("Dados dos medidores Hemera carregados com sucesso")
                return df_atributos
        except Exception as e:
            st.error("Erro ao carregar dados dos medidores Hemera.")
            return pd.DataFrame()
    
    # Carrega potencia instalada dos equipamentos
    def obter_potencia_instalada(selecao):
        try:
            base_path = Path(__file__).resolve().parent
            db_path = base_path / "Cadastro" / "Dados.db"
            with sql.connect(db_path) as conexao:
                query = """
                    SELECT potencia_instalada 
                    FROM dados_tecnicos 
                    WHERE codigo_trafo_alimentador = ?
                """
                potencia = pd.read_sql_query(query, conexao, params=(selecao,)).iloc[0, 0]
                logger.info(f"Potência instalada para {selecao} obtida com sucesso: {potencia} kW")
                return potencia
        except Exception as e:
            st.error("Erro ao obter potência instalada para o equipamento selecionado")
            return None

    selecao = st.sidebar.selectbox("Selecione um Equipamento", df_equipamentos, index=2)
    selecao_str = str(selecao)
    EAE = selecao_str + '-EAE'
    EAR = selecao_str + '-EAR'
    ERE = selecao_str + '-ERE'
    ERR = selecao_str + '-ERR'
    
    potencia_instalada = obter_potencia_instalada(selecao)
 
    # Substituir consulta para buscar diretamente na tabela medidores_hemera
    try:
        base_path = Path(__file__).resolve().parent
        db_path = base_path / "Cadastro" / "Dados.db"
        with sql.connect(db_path) as conexao:
            query_EAE = "SELECT descricao FROM medidores_hemera WHERE Codigo = ?"
            descricao = pd.read_sql_query(query_EAE, conexao, params=(EAE,)).iloc[0, 0]
            query_EAR = "SELECT descricao FROM medidores_hemera WHERE Codigo = ?"
            descricao_saida = pd.read_sql_query(query_EAR, conexao, params=(EAR,)).iloc[0, 0]
            query_ERE = "SELECT descricao FROM medidores_hemera WHERE Codigo = ?"
            descricao_Q = pd.read_sql_query(query_ERE, conexao, params=(ERE,)).iloc[0, 0]
            query_ERR = "SELECT descricao FROM medidores_hemera WHERE Codigo = ?"
            descricao_saida_Q = pd.read_sql_query(query_ERR, conexao, params=(ERR,)).iloc[0, 0]
            logger.info("Consultas realizadas com sucesso na tabela medidores_hemera")
           
    except Exception as e:
        st.error("Erro ao buscar descrições na tabela medidores_hemera")
        st.stop()

    # Consultar a base de dados para importar os dados
    def consultar_demandas_por_tabela(descricao, descricao_saida, descricao_Q, descricao_saida_Q, potencia_instalada):
        base_path = Path(__file__).resolve().parent
        db_path = base_path / "DataBase" / "Medicoes.db"
        try:
            with sql.connect(str(db_path)) as conexao:
                cursor = conexao.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tabelas = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Erro ao listar tabelas do banco: {e}")
            return pd.DataFrame()

        resultados = []
        try:
            with sql.connect(str(db_path)) as conexao:
                for tabela in tabelas:
                    try:
                        query = f"""
                            SELECT 
                                [Data_Hora] AS Data_Hora, 
                                [{descricao}] AS descricao,
                                [{descricao_saida}] AS descricao_saida,
                                [{descricao_Q}] AS descricao_Q,
                                [{descricao_saida_Q}] AS descricao_saida_Q
                            FROM {tabela}
                        """
                        df = pd.read_sql_query(query, conexao)
                        # Check if Data_Hora exists before further processing
                        if 'Data_Hora' not in df.columns:
                            logger.warning(f"Tabela {tabela} não retornou coluna 'Data_Hora'. Colunas retornadas: {df.columns.tolist()}")
                            continue
                        df["Data_Hora"] = pd.to_datetime(df["Data_Hora"], errors='coerce')
                        df["tabela_origem"] = tabela
                        resultados.append(df)
                    except Exception as e:
                        logger.warning(f"Erro ao consultar a tabela {tabela}: {e}")

            if resultados:
                base = pd.concat(resultados, ignore_index=True)
                # Only process if 'Data_Hora' exists
                if 'Data_Hora' not in base.columns:
                    logger.error("Nenhuma das tabelas retornou coluna 'Data_Hora'.")
                    return pd.DataFrame()
                base['Data_Hora']= pd.to_datetime(base['Data_Hora'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                # Garantir que o índice seja convertido para datetime e ordenado cronologicamente
                base.index = pd.to_datetime(base['Data_Hora'], errors='coerce')
                base.set_index("Data_Hora", inplace=True)
                base = base[~base.index.duplicated(keep='first')]
                
                # Calcular as colunas necessárias
                base['P'] = base['descricao'] - base['descricao_saida']
                base['PQ'] = base['descricao_Q'] - base['descricao_saida_Q']
                base['S'] = np.sqrt((base['P']**2) + (base['PQ']**2))
                base['fp'] = base['P'] / base['S']
                base['ultrapassagem'] = (base['S'] / potencia_instalada >= 1.00).astype(int)
                return base
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Erro geral na consulta às tabelas de demanda: {e}")            
            st.error("Erro ao consultar as tabelas de demanda.")
            return pd.DataFrame()

    base = consultar_demandas_por_tabela(descricao, descricao_saida, descricao_Q, descricao_saida_Q, potencia_instalada)
    #base['Data_Hora']= pd.to_datetime(base['Data_Hora'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    

    if base.empty:
        st.error("Nenhum dado encontrado nas tabelas de demanda com os filtros aplicados.")
        st.stop()
    #base.set_index("Data_Hora", inplace=True)
    # Utilizar base diretamente sem recalcular colunas já existentes
    valor_maximo_P = base['P'].max()
    valor_maximo_S = round(base['S'].max(), 2)
    valor_minimo_P = base['P'].min()
    valor_media_p = round(base['P'].mean(), 0)

    dados_sem_zeros = base['P'].replace([np.inf, -np.inf], np.nan).dropna()
    dados_sem_zeros = dados_sem_zeros[dados_sem_zeros != 0].astype(int)
    valor_minimo_P_sem_zero = dados_sem_zeros.min()
    
    if not dados_sem_zeros.empty:
        desvio_padrao_P = dados_sem_zeros.std()
        if pd.notna(desvio_padrao_P):
            desvio_padrao_P = int(desvio_padrao_P)
        else:
            desvio_padrao_P = 0  # Valor padrão caso o desvio padrão seja NaN
    else:
        desvio_padrao_P = 0
    minimo_desvio = round(valor_media_p - 3.0 * desvio_padrao_P, 0)
    fp = round(valor_media_p / base['S'].mean(), 2)

    dados_filtrados_desvio = np.where(base['P'] < valor_media_p + 3 * desvio_padrao_P, base['P'], np.nan)
    valor_maximo_filtrado = np.nanmax(dados_filtrados_desvio)

    if valor_maximo_P / valor_maximo_filtrado > 1.10:
        valor_maximo_P = valor_maximo_filtrado

    max_date_time_index = base.index[base['P'] == valor_maximo_P].tolist()
    min_date_time_index = base.index[base['P'] == valor_minimo_P].tolist()

    valor_Q_correspondente = base.at[max_date_time_index[0], 'PQ'] if max_date_time_index else None
    valor_Q_correspondente_minimo = base.at[min_date_time_index[0], 'PQ'] if min_date_time_index else None

    valor_media_Q = round(base['PQ'].mean(), 0)
    valor_maximo_S_filtrado = np.sqrt((valor_maximo_P**2) + (valor_media_Q**2))
    carregamento = (valor_maximo_S_filtrado / potencia_instalada) * 100
    carregamento_percentual = f'{carregamento:.2f}%'

    if valor_minimo_P < minimo_desvio:
        valor_minimo_P = minimo_desvio

    Equipamento = descricao.replace("-EAE", "")
    st.sidebar.subheader(Equipamento)

    # Métricas
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric('Valor Máximo da P. Ativa:', valor_maximo_P)
    col2.metric('Carregamento:', carregamento_percentual, round(potencia_instalada, 0))
    col3.metric('Valor Mínimo da P. Ativa:', valor_minimo_P)
    col4.metric('Fator de Potência:', fp)
    col5.metric('Qtd de horas ultrapassagem', base['ultrapassagem'].sum())

    # Criar gráfico
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=base.index, y=base['P'], name='Potencia Ativa - kW', line=dict(color='Navy')))
    fig.add_trace(go.Scatter(x=base.index, y=base['PQ'], name='Potencia Reativa - kVAr', line=dict(color='Tomato')))
    fig.add_trace(go.Scatter(x=base.index, y=base['S'], name='Potencia Aparente - kVA', line=dict(color='DarkCyan')))

    # Configurar layout do gráfico
    x_range = [base.index[0], base.index[-1]]
    y_range = [min(base['P'].min(), 0), 0]

    fig.update_layout(
        title="Gráfico Anual: " + Equipamento,
        xaxis_title="Data",
        yaxis_title="Valor",
        width=1480,
        height=600,
        shapes=[
            dict(
                type="rect",
                xref="x",
                yref="y",
                x0=x_range[0],
                y0=y_range[0],
                x1=x_range[1],
                y1=y_range[1],
                fillcolor="lightpink",
                opacity=0.4,
                layer="below",
                line_width=0
            )
        ]
    )

    # Adicionar anotações e linhas
    fig.add_annotation(
        text="Inversão de Fluxo",
        xref="paper",
        yref="paper",
        x=0,
        y=-0.02,
        font=dict(color="black"),
        showarrow=False
    )

    fig.add_shape(
        type="line",
        x0=base.index[0],
        y0=potencia_instalada,
        x1=base.index[-1],
        y1=potencia_instalada,
        line=dict(color="red", width=3.0)
    )

    fig.add_shape(
        type="line",
        x0=base.index[0],
        y0=valor_minimo_P,
        x1=base.index[-1],
        y1=valor_minimo_P,
        line=dict(color="red", width=1.7, dash="dash")
    )

    fig.add_shape(
        type="line",
        x0=base.index[0],
        y0=valor_maximo_P,
        x1=base.index[-1],
        y1=valor_maximo_P,
        line=dict(color="black", width=1.5, dash="dash")
    )

    st.plotly_chart(fig)

    # Sidebar informações
    st.sidebar.markdown("#### Potência Máxima")
    st.sidebar.write(f"**Potência Ativa:** {valor_maximo_P} kW")
    st.sidebar.write(f"**Potência Reativa:** {valor_Q_correspondente} kVAR")

    if max_date_time_index:
        data_potencia_maxima = max_date_time_index[0]
        data_formatada = data_potencia_maxima.strftime('%d-%m-%Y %H:%M')
        st.sidebar.write(f"**Data:** {data_formatada}")


        # Sidebar informações
        st.sidebar.markdown("#### Potência Máxima")
        st.sidebar.write(f"**Potência Ativa:** {valor_maximo_P} kW")
        st.sidebar.write(f"**Potência Reativa:** {valor_Q_correspondente} kVAR")

        if max_date_time_index:
            # Corrige erro de KeyError: 'Data_Hora' ao acessar pelo índice
            try:
                if 'Data_Hora' in base.columns:
                    data_potencia_maxima = base.at[max_date_time_index[0], 'Data_Hora']
                else:
                    data_potencia_maxima = max_date_time_index[0]
                if isinstance(data_potencia_maxima, pd.Timestamp):
                    data_formatada = data_potencia_maxima.strftime('%d-%m-%Y %H:%M')
                else:
                    try:
                        data_formatada = pd.to_datetime(data_potencia_maxima).strftime('%d-%m-%Y %H:%M')
                    except Exception:
                        data_formatada = str(data_potencia_maxima)
                st.sidebar.write(f"**Data:** {data_formatada}")
            except Exception as e:
                logger.warning(f"Erro ao formatar data_potencia_maxima: {e}")

    st.sidebar.divider()

    st.sidebar.markdown("#### Potência Mínima")
    st.sidebar.write(f"**Potência Ativa:** {valor_minimo_P} kW")
    st.sidebar.write(f"**Potência Reativa:** {valor_Q_correspondente_minimo} kVAr")

    if min_date_time_index:
        # Corrige erro de KeyError: 'Data_Hora' ao acessar pelo índice
        try:
            if 'Data_Hora' in base.columns:
                data_potencia_minima = base.at[min_date_time_index[0], 'Data_Hora']
            else:
                data_potencia_minima = min_date_time_index[0]
            if isinstance(data_potencia_minima, pd.Timestamp):
                data_formatada_min = data_potencia_minima.strftime('%d-%m-%Y %H:%M')
            else:
                try:
                    data_formatada_min = pd.to_datetime(data_potencia_minima).strftime('%d-%m-%Y %H:%M')
                except Exception:
                    data_formatada_min = str(data_potencia_minima)
            st.sidebar.write(f"**Data:** {data_formatada_min}")
        except Exception as e:
            logger.warning(f"Erro ao formatar data_potencia_minima: {e}")


    # Gráfico de demanda máxima histórica
    st.divider()
    st.subheader('Gráfico da Máxima Demanda Histórica')

    if "TR" in selecao:
        selecao_2 = selecao
    else:
        selecao_2 = 'AL-' + selecao

    filtered_data = df_maxima[df_maxima['Cód. do Trafo/Alimentador'] == selecao_2]
    mes_columns = [col for col in filtered_data.columns if col.split()[0] in [
        'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
        'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
    ]]

    for col in mes_columns:
        filtered_data.loc[:, col] = pd.to_numeric(filtered_data[col], errors='coerce')

    filtered_data_melted = filtered_data.melt(
        id_vars=['Descrição', 'Cód. do Trafo/Alimentador'],
        value_vars=mes_columns,
        var_name='Mês',
        value_name='Valor'
    )

    fig3 = px.bar(
        filtered_data_melted,
        x='Mês',
        y='Valor',
        color='Cód. do Trafo/Alimentador',
        text='Valor',
        title=f'Gráfico de Barras para {selecao}'
    )

    fig3.update_layout(
        xaxis_title="Meses",
        yaxis_title="Carregamento (%)",
        width=1500,
        height=680
    )

    st.plotly_chart(fig3)

    # Gráfico da curva diária
    st.divider()
    st.subheader('Gráfico da Curva Diária')

    colu1, colu2 = st.columns(2)

    with colu1:
        mes_selecionado = st.selectbox('Selecione o mês:', ['Todos', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], index=1)

    with colu2:
        ano_selecionado = st.selectbox('Selecione o ano:', [2023, 2024, 2025], index=1)

    fig2 = go.Figure()

    if mes_selecionado == 'Todos':
        dados_filtrados = base[base.index.year == ano_selecionado]
    else:
        dados_filtrados = base[(base.index.month == mes_selecionado) & (base.index.year == ano_selecionado)]

    for dia in dados_filtrados.index.day.unique():
        dados_dia = dados_filtrados[dados_filtrados.index.day == dia]
        fig2.add_trace(go.Scatter(x=dados_dia.index.hour, y=dados_dia['P'], name=f'Dia {dia}'))

    fig2.update_layout(
        title="Curva Diária: " + Equipamento,
        xaxis_title="Hora",
        yaxis_title="Potência Ativa - kW",
        xaxis=dict(type="linear"),  # Configurar eixo X como tipo linear para horas
        width=1480,
        height=500
    )

    st.plotly_chart(fig2)

    # Exportação dos dados
    dados_filtrados = dados_filtrados.drop(columns=['S', 'fp', 'ultrapassagem'])
    buffer = io.StringIO()
    dados_filtrados.to_csv(buffer, sep=';', encoding='latin-1')
    buffer.seek(0)
    file_name = f"exportado/{Equipamento}.csv"

    st.download_button(
        label="Download",
        data=buffer.getvalue(),
        file_name=file_name,
        mime="text/csv"
    )

    st.text("\nDesenvolvido por Arthur Williams")

# Garantir que importa_base seja chamada corretamente
if __name__ == "__main__":
    pagina_principal.importa_base()
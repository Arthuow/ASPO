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
import logging
import duckdb
import gdown
from pathlib import Path

# Definir logger global
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

# Configuração da página Streamlit
st.set_page_config(page_title="Energisa Mato Grosso", page_icon='icone', layout='wide')

# Cache para dados de demanda máxima
@st.cache_data()
def carregar_dados_demanda_maxima():
    try:
        df_maxima = pd.read_excel("input/Demanda_Máxima_Não_Coincidente_Historica.xlsx",
                                sheet_name="Potência Aparente",
                                engine="openpyxl")
        logger.info("Dados de demanda máxima carregados com sucesso")
        return df_maxima
    except Exception as e:
        logger.error(f"Erro ao carregar dados de demanda máxima: {str(e)}")
        st.error("Erro ao carregar dados de demanda máxima. Verifique se o arquivo existe no diretório input/")
        return pd.DataFrame()

# Carregar dados de demanda máxima
df_maxima = carregar_dados_demanda_maxima()

@st.cache_data
def importa_base():
    logger.info("Importando Base de Dados do DuckDB")
    try:
        base_path = Path(__file__).resolve().parent
        db_path = base_path / "Base/medicoes.duckdb"
        # Verifica se o arquivo existe, se não, baixa do Google Drive
        if not db_path.exists():
            st.info("Baixando base de dados do Google Drive. Aguarde...")
            url = "https://drive.google.com/uc?id=1jXdKM46ZRYlQbK1LB-xa8Ft-lbJM4BG-"
            gdown.download(url, str(db_path), quiet=False)
            logger.info("Arquivo medicoes.duckdb baixado do Google Drive.")
        # Conectar ao DuckDB com configurações otimizadas
        conn = duckdb.connect(str(db_path), read_only=True)

        # Otimizar a query usando DuckDB
        query = """
        WITH filtered_data AS (
            SELECT 
                Data_Hora,
                ponto_medicao,
                AVG(valor) as valor
            FROM DemandaDiaria 
            WHERE Data_Hora < CURRENT_DATE - INTERVAL '1' DAY
            GROUP BY Data_Hora, ponto_medicao
        )
        SELECT * FROM filtered_data
        ORDER BY Data_Hora
        """

        # Ler dados usando DuckDB com otimizações
        df_base = conn.execute(query).df()

        # Converter Data_Hora para datetime de forma otimizada
        df_base['Data_Hora'] = pd.to_datetime(df_base['Data_Hora'])

        # Otimizar o pivot usando pivot_table com aggfunc='mean'
        df_base = df_base.pivot_table(
            index='Data_Hora',
            columns='ponto_medicao',
            values='valor',
            aggfunc='mean'
        )

        if df_base.empty:
            raise ValueError("Nenhum dado encontrado após o processamento")

        logger.info(f"Base de dados importada com sucesso. Shape: {df_base.shape}")
        return df_base

    except Exception as e:
        logger.error(f"Erro ao importar base de dados: {str(e)}")
        st.error(f"Erro ao importar base de dados: {str(e)}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

@st.cache_data(ttl=3600)  # Cache por 1 hora
def importar_base_equipamentos():
    try:
        df_equipamentos = pd.read_excel('input/Códigos dos Equipamentos.xlsx',
                                      sheet_name='Códigos dos Equipamentos')
        df_equipamentos['Descricao'] = df_equipamentos['Descricao'].astype(str)
        logger.info("Importação da base de equipamentos concluída")
        return df_equipamentos
    except Exception as e:
        logger.error(f"Erro ao importar base de equipamentos: {str(e)}")
        return None

@st.cache_data(ttl=3600)  # Cache por 1 hora
def carregar_dados_tecnicos():
    try:
        df_atributos = pd.read_excel('input/Tabela informativa.xlsx', sheet_name="Dados")
        df_atributos.dropna(subset=['Codigo'], inplace=True)

        df_dados_tecnicos = pd.read_excel('input/Tabela informativa.xlsx', sheet_name='Dados Técnicos')
        df_dados_tecnicos['Cód. do Trafo/Alimentador'] = df_dados_tecnicos['Cód. do Trafo/Alimentador'].astype(str)

        logger.info("Arquivo Tabela informativa.xlsx lido com sucesso")
        return df_atributos, df_dados_tecnicos
    except Exception as e:
        logger.error(f"Erro ao ler arquivo Tabela informativa.xlsx: {str(e)}")
        st.error(f"Erro ao ler arquivo Tabela informativa.xlsx: {str(e)}")
        return None, None

# Carregar dados técnicos
df_atributos, df_dados_tecnicos = carregar_dados_tecnicos()
if df_atributos is None or df_dados_tecnicos is None:
    st.stop()

# Interface do usuário
st.header('Energisa Mato Grosso - ASPO')
st.markdown("Assessoria de Planejamento e Orçamento")

# Importar base de equipamentos
df_equipamentos = importar_base_equipamentos()
if df_equipamentos is None:
    st.error("Não foi possível carregar a base de equipamentos")
    st.stop()

# Interface do usuário
st.sidebar.header("Equipamento Elétrico")
selecao = st.sidebar.selectbox("Selecione um Equipamento", df_equipamentos, index=1)

# Verificar se selecao é válido
if selecao is None:
    st.error("Por favor, selecione um equipamento válido")
    st.stop()

# Processamento dos dados
try:
    selecao_str = str(selecao)
    EAE = selecao_str + '-EAE'
    EAR = selecao_str + '-EAR'
    ERE = selecao_str + '-ERE'
    ERR = selecao_str + '-ERR'
    logger.info(f"Processando equipamento: {selecao_str}")
except Exception as e:
    logger.error(f"Erro ao processar seleção do equipamento: {str(e)}")
    st.error("Erro ao processar seleção do equipamento")
    st.stop()

try:
    potencia_instalada = df_dados_tecnicos.loc[df_dados_tecnicos['Cód. do Trafo/Alimentador']==selecao_str,'Potencia Instalada'].values[0]
    logger.info(f"Potência instalada para {selecao_str}: {potencia_instalada}")
except Exception as e:
    logger.error(f"Erro ao obter potência instalada: {str(e)}")
    st.error(f"Erro ao obter potência instalada para o equipamento selecionado")
    st.stop()

# Processar descrições
try:
    indice_entrada = df_atributos.loc[df_atributos['Codigo'] == EAE, 'Codigo'].index[0]
    descricao = str(df_atributos.loc[indice_entrada, 'descricao'])

    indice_saida = df_atributos.loc[df_atributos['Codigo'] == EAR, 'Codigo'].index[0]
    descricao_saida = str(df_atributos.loc[indice_saida, 'descricao'])

    indice_entrada_Q = df_atributos.loc[df_atributos['Codigo'] == ERE, 'Codigo'].index[0]
    descricao_Q = str(df_atributos.loc[indice_entrada_Q, 'descricao'])

    indice_saida_Q = df_atributos.loc[df_atributos['Codigo'] == ERR, 'Codigo'].index[0]
    descricao_saida_Q = str(df_atributos.loc[indice_saida_Q, 'descricao'])
except Exception as e:
    logger.error(f"Erro ao processar descrições: {str(e)}")
    st.error("Erro ao processar dados do equipamento")
    st.stop()

# Importar e processar base de dados
base = importa_base()
if base is None:
    st.error("Não foi possível carregar a base de dados. Por favor, verifique o banco de dados DuckDB.")
    st.stop()

# Processar dados
base = pd.DataFrame(base, columns=[descricao, descricao_saida, descricao_Q, descricao_saida_Q])
for col in [descricao, descricao_saida, descricao_Q, descricao_saida_Q]:
    base[col] = pd.to_numeric(base[col], errors='coerce')

# Calcular valores
base['P'] = base[descricao] - base[descricao_saida]
base['PQ'] = base[descricao_Q] - base[descricao_saida_Q]
base['S'] = np.sqrt((base['P']**2) + (base['PQ']**2))
base['fp'] = base['P'] / base['S']
base['ultrapassagem'] = (base['S'] / potencia_instalada >= 1.00).astype(int)

# Calcular estatísticas
valor_maximo_P = base['P'].max()
valor_maximo_S = round(base['S'].max(), 2)
valor_minimo_P = base['P'].min()
valor_media_p = round(base['P'].mean(), 0)

# Tratar valores não finitos antes da conversão para inteiro
dados_sem_zeros = base['P'].replace([np.inf, -np.inf], np.nan).dropna()
dados_sem_zeros = dados_sem_zeros[dados_sem_zeros != 0].astype(int)
valor_minimo_P_sem_zero = dados_sem_zeros.min()
desvio_padrao_P = int(dados_sem_zeros.std())
minimo_desvio = round(valor_media_p - 3.0 * desvio_padrao_P, 0)
fp = round(valor_media_p / base['S'].mean(), 2)

# Calcular potência máxima filtrada
dados_filtrados_desvio = np.where(base['P'] < valor_media_p + 3 * desvio_padrao_P, base['P'], np.nan)
valor_maximo_filtrado = np.nanmax(dados_filtrados_desvio)

if valor_maximo_P / valor_maximo_filtrado > 1.10:
    valor_maximo_P = valor_maximo_filtrado

# Encontrar datas dos valores máximos e mínimos
max_date_time_index = base.index[base['P'] == valor_maximo_P].tolist()
min_date_time_index = base.index[base['P'] == valor_minimo_P].tolist()

# Calcular valores Q correspondentes
valor_Q_correspondente = base.at[max_date_time_index[0], 'PQ'] if max_date_time_index else None
valor_Q_correspondente_minimo = base.at[min_date_time_index[0], 'PQ'] if min_date_time_index else None

# Calcular carregamento
valor_media_Q = round(base['PQ'].mean(), 0)
valor_maximo_S_filtrado = np.sqrt((valor_maximo_P**2) + (valor_media_Q**2))
carregamento = (valor_maximo_S_filtrado / potencia_instalada) * 100
carregamento_percentual = f'{carregamento:.2f}%'

# Ajustar valor mínimo se necessário
if valor_minimo_P < minimo_desvio:
    valor_minimo_P = minimo_desvio

# Interface do usuário
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

st.sidebar.divider()

st.sidebar.markdown("#### Potência Mínima")
st.sidebar.write(f"**Potência Ativa:** {valor_minimo_P} kW")
st.sidebar.write(f"**Potência Reativa:** {valor_Q_correspondente_minimo} kVAr")

if min_date_time_index:
    data_potencia_minima = min_date_time_index[0]
    data_formatada_min = data_potencia_minima.strftime('%d-%m-%Y %H:%M')
    st.sidebar.write(f"**Data:** {data_formatada_min}")

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
    ano_selecionado = st.selectbox('Selecione o ano:', [2023, 2024], index=1)

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

if __name__ == "__main__":
    app = App()

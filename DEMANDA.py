#Projeto Energisa de Leitura de dados de Demanda
import math
import time
import datetime
import pandas as pd
import os
import warnings
import openpyxl
warnings.filterwarnings("ignore")
import streamlit as st
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import gc
from datetime import datetime, timedelta
import streamlit.components.v1 as components

df_maxima = pd.read_excel("Valores_maximos_P_meses_2022.xlsx", sheet_name="Potência Ativa Máxima")
print(df_maxima)

@st.cache_data
def importa_base():
    print("Importando Base de Dados Agrupada\n")
    url_base = r"C:\Users\Engeselt\Documents\GitHub\ASPO\Medição Agrupada.csv"
    df_base = pd.read_csv("Medição Agrupada.csv", sep=";", encoding='latin-1').set_index(['DATA_HORA'])
    print(df_base)
    print("\nImportação Concluída")
    return df_base

df_equipamentos = None
def importar_base_equipamentos():
    global df_equipamentos
    if df_equipamentos is None:
        print("Importando base de equipamentos:")
        #url_equipamentos = r"C:\Users\Engeselt\Documents\Códigos dos Equipamentos.xlsx"
        df_equipamentos = pd.read_excel('Códigos dos Equipamentos.xlsx', sheet_name='Códigos dos Equipamentos')
        print(df_equipamentos)
        print("\nImportação Concluída")
        df_equipamentos.info()

########################################################################################################################
# LENDO ARQUIVO EM EXCEL
#url_atributos = r"C:\Users\Engeselt\Documents\Tabela informativa.xlsx"
df_atributos = pd.read_excel('Tabela informativa.xlsx',sheet_name = "Dados")
df_atributos.dropna(subset=['Codigo'], inplace=True)
df_dados_tecnicos = pd.read_excel('Tabela informativa.xlsx',sheet_name='Dados Técnicos')
print("\n Tabela para realizar o procv importada")
print(df_dados_tecnicos)

import streamlit as st

importar_base_equipamentos()
st.set_page_config(page_title="Energisa Mato Grosso",page_icon='icone',layout='wide')
st.header('Energisa Mato Grosso - ASPO')
st.markdown("Assessoria de Planejamento e Orçamento")
st.sidebar.header("Equipamento Elétrico")
selecao = st.sidebar.selectbox("Selecione um Equipamento", df_equipamentos)
EAE = selecao + '-EAE'
EAR = selecao + '-EAR'
ERE = selecao + '-ERE'
ERR = selecao + '-ERR'

potencia_instalada =df_dados_tecnicos.loc[df_dados_tecnicos['Cód. do Trafo/Alimentador']==selecao,'Potencia Instalada'].values[0]
print('Potencia Instalada:',potencia_instalada)

###Potencia Ativa#####

indice_entrada = df_atributos.loc[df_atributos['Codigo'] == EAE, 'Codigo'].index[0]
print(indice_entrada)
descricao = (df_atributos.loc[indice_entrada, 'descricao'])
descricao=str(descricao)

print(descricao)
indice_saida = df_atributos.loc[df_atributos['Codigo'] == EAR, 'Codigo'].index[0]
print(indice_entrada)
descricao_saida = df_atributos.loc[indice_saida, 'descricao']
descricao_saida=str(descricao_saida)
print(descricao_saida)

####Potencia Reativa#####

indice_entrada_Q = df_atributos.loc[df_atributos['Codigo'] == ERE, 'Codigo'].index[0]
descricao_Q = (df_atributos.loc[indice_entrada_Q, 'descricao'])
descricao_Q=str(descricao_Q)
indice_saida_Q = df_atributos.loc[df_atributos['Codigo'] == ERR, 'Codigo'].index[0]
descricao_saida_Q = df_atributos.loc[indice_saida_Q, 'descricao']
descricao_saida_Q=str(descricao_saida_Q)

base=importa_base()
base.index = pd.to_datetime(base.index)
data_d_minus_1 = datetime.today() - timedelta(days=1)
base = base[base.index < data_d_minus_1]

base = pd.DataFrame(base,columns=[descricao,descricao_saida,descricao_Q,descricao_saida_Q])
base[descricao] = base[descricao].astype(float)
base[descricao_saida] = base[descricao_saida].astype(float)
base[descricao_Q] = base[descricao_Q].astype(float)
base[descricao_saida_Q] = base[descricao_saida_Q].astype(float)
base['P'] = base[descricao]-base[descricao_saida]
base['PQ'] = base[descricao_Q]-base[descricao_saida_Q]
base['S'] = np.sqrt((base['P']**2)+(base['PQ']**2))
base['fp'] = base['P']/base['S']
base['ultrapassagem'] = base['S']/potencia_instalada >= 1.00
base['ultrapassagem'] = base['ultrapassagem'].astype(int)
print(base)
valor_maximo_P = max(base['P'])
valor_maximo_S = max(base['S'])
valor_maximo_S = round(float(valor_maximo_S),2)
valor_minimo_P = min(base['P'])
valor_media_p = round(np.mean(base['P']),0)
print(valor_media_p)

dados_sem_zeros = np.array([x for x in base['P'] if x!=0]).astype(int)
print("Dados sem o zero\n", dados_sem_zeros)

valor_minimo_P_sem_zero = min(dados_sem_zeros).astype(int)
print("Valor Minimo sem o zero:\n",valor_minimo_P_sem_zero)

desvio_padrao_P = int(np.std(dados_sem_zeros))
print("desvio Padrão: ",desvio_padrao_P)

minimo_desvio = round(valor_media_p-3*desvio_padrao_P,0)
print("Valor minimo por desvio padrão:",minimo_desvio )
fp = round(valor_media_p/(np.mean(base['S'])),2)
#print(base)

#######  Potencia máxima considerada ##################
dados_filtrados_desvio = np.where(base['P'] < valor_media_p + 3 * desvio_padrao_P, base['P'], np.nan)
valor_maximo_filtrado = np.nanmax(dados_filtrados_desvio)
print('Valor Máximo Filtrado:', valor_maximo_filtrado)
print('Valor máximo',valor_maximo_P)
if valor_maximo_P/valor_maximo_filtrado>1.10:
    valor_maximo_P = valor_maximo_filtrado
else:
    valor_maximo_P = valor_maximo_P

################################ DEFINIÇÃO DO CARREGAMENTO #########################################################
valor_media_Q = round(np.mean(base['PQ']),0)
print("Valor médio do PQ",valor_media_Q)
valor_maximo_S_filtrado = np.sqrt((valor_maximo_P**2)+(valor_media_Q**2))
carregamento = (valor_maximo_S_filtrado/potencia_instalada)*100
carregamento_percentual = '{:.2f}%'.format(carregamento)
print("Carregamento (%):",carregamento_percentual)

#####################################################################################################################
############################### CONTAGEM DE HORAS ACIMA DA POTENCIA NOMINAL #########################################
#####Potencia mínima considerada ####

if valor_minimo_P < minimo_desvio:
    valor_minimo_P = minimo_desvio
else:
    valor_minimo_P

################################## PLOTAGEM DOS GRÀFICOS #######################################################


Equipamento = descricao.replace("-EAE","")
st.sidebar.subheader(Equipamento)

# Determinando as coordenadas do retângulo abaixo do eixo X
x_range = [base.index[0], base.index[-1]]
y_range = [base['P'].min(), 0]
y_range[0] = min(y_range[0], 0)
# Adicionando o retângulo no layout
gc.collect()
#tab1=st.tabs(['Gráficos'])
col1,col2,col3,col4, col5 = st.columns(5)
col1.metric('Valor Máximo da P. Ativa:',valor_maximo_P)
col2.metric('Carregamento:',carregamento_percentual,round(potencia_instalada,0))
col3.metric('Valor Mínimo da P. Ativa:',valor_minimo_P)
col4.metric('Fator de Potência:',fp)
col5.metric('Qtd de horas ultrapassagem',sum(base['ultrapassagem']))
fig = go.Figure()
fig.add_trace(go.Scatter(x=base.index, y=base['P'], name='Potencia Ativa - kW', line=dict(color='Navy')))
fig.add_trace(go.Scatter(x=base.index, y=base['PQ'], name='Potencia Reativa - kVAr', line=dict(color='Tomato')))
fig.add_trace(go.Scatter(x=base.index, y=base['S'], name='Potencia Aparente - kVA', line=dict(color='DarkCyan')))
fig.update_layout(title="Gráfico Anual: " + Equipamento,xaxis_title="Data",yaxis_title="Valor",width=1480,height=600,shapes=[dict(type="rect",xref="x",yref="y",x0=x_range[0], y0=y_range[0], x1=x_range[-1],y1=y_range[1],
                  fillcolor="lightpink",opacity=0.4,layer="below",line_width=0)])
fig.add_annotation(text="Inversão de Fluxo",xref="paper",yref="paper",x=0,y=-0.02,font=dict(color="black"),showarrow=False)
fig.add_shape(type="line",x0=base.index[0], y0=potencia_instalada,x1=base.index[-1], y1=potencia_instalada, line=dict(color="red", width=3.0))
fig.add_shape(type="line",x0=base.index[0], y0=valor_minimo_P,x1=base.index[-1], y1=valor_minimo_P, line=dict(color="red", width=1.7, dash="dash"))
fig.add_shape(type="line",x0=base.index[0], y0=valor_maximo_P,x1=base.index[-1], y1=valor_maximo_P, line=dict(color="black", width=1.5, dash="dash"))
st.plotly_chart(fig)

########################################################################################################################

#########################################3 DEMANDA MÁXIMAS DE 2022 #####################################################
st.divider()

#df_maxima_2 = pd.read_excel("Valores_maximos_P_meses.xlsx", sheet_name="Potência Ativa Máxima")
#st.header('Demandas Máximas de 2022')
if "TR" in selecao:
    selecao_2 = selecao
else:
    selecao_2 = 'AL-'+selecao

st.subheader('Gráfico da Máxima Demanda de 2022')
# Seleção do trafo/alimentador
filtered_data = df_maxima[df_maxima['Cód. do Trafo/Alimentador'] == selecao_2]
mes_atual = datetime.now()
mes_columns = [col for col in filtered_data.columns if col.split()[0] in ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']]

# Converter as colunas de meses para números (float)
for col in mes_columns:
    filtered_data.loc[:, col] = pd.to_numeric(filtered_data[col], errors='coerce')

# Derreter os dados apenas nas colunas de meses
filtered_data_melted = filtered_data.melt(id_vars=['Descrição', 'Cód. do Trafo/Alimentador'], value_vars=mes_columns, var_name='Mês', value_name='Valor')

# Criar o gráfico de barras usando Plotly Express
fig3 = px.bar(filtered_data_melted, x='Mês', y='Valor', color='Cód. do Trafo/Alimentador', text='Valor', title=f'Gráfico de Barras para {selecao}')

fig3.update_layout(xaxis_title="Meses",yaxis_title="Carregamento (%)",width=1500, height=680)
# Mostrar o gráfico usando Streamlit
st.plotly_chart(fig3)

########################################################################################################################


st.divider()
st.subheader('Gráfico da Curva Diária')
colu1,colu2 = st.columns(2)
with colu1:
    mes_selecionado =st.selectbox('Selecione o mês:',[1,2,3,4,5,6,7,8,9,10,11,12])
with colu2:
    ano_selecionado = st.selectbox('Selecione o ano:',[2022])

fig2 = go.Figure()
base['DATA_HORA_converted'] = pd.to_datetime(base.index, errors='coerce')
print(base['DATA_HORA_converted'])
base.index = pd.to_datetime(base.index)

dados_filtrados = base[(base.index.month == mes_selecionado) & (base.index.year == ano_selecionado)]
for dia in dados_filtrados.index.day.unique():
    dados_dia = dados_filtrados[dados_filtrados.index.day == dia]
    fig2.add_trace(go.Scatter(x=dados_dia.index.hour, y=dados_dia['P'], name=f'Dia {dia}'))

x_range = [dados_filtrados.index.min(), dados_filtrados.index.max()]
y_range = [dados_filtrados['P'].min(), dados_filtrados['P'].max() + 10]
fig2.update_layout(title="Curva Diária: " + Equipamento,xaxis_title="Hora",yaxis_title="Potência Ativa - kW",width=1480,height=500,shapes=[dict(type="rect",xref="x",yref="y",x0=x_range[0], y0=y_range[0], x1=x_range[1],y1=y_range[1],
                  fillcolor="lightpink",opacity=0.4,layer="below",line_width=0)])
st.plotly_chart(fig2)
st.text("\nDesenvolvido por Arthur Williams")

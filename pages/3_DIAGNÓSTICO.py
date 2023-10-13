import os

import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
st.set_page_config(page_title="Energisa Mato Grosso",page_icon='icone',layout='wide')
df_maxima = pd.read_excel("Valores_maximos_P_meses.xlsx", sheet_name="Potência Ativa Máxima")
df_maxima_2 = pd.read_excel("Valores_maximos_P_meses.xlsx", sheet_name="Potência Ativa Máxima")
st.header('Diagnóstico do Sistema Elétrico 2023')

################################# DIAGNÓSTICO TRANSFORMADOR ##########################################################

st.divider()
st.subheader('Diagnóstico Dos Transformadores')
df_dados_com_meses_anos = df_maxima
df_dados_com_meses_anos = df_dados_com_meses_anos.loc[df_dados_com_meses_anos["Tipo"]=='Transformador']
df_dados_com_meses_anos['Cód. do Trafo/Alimentador']=df_dados_com_meses_anos['Cód. do Trafo/Alimentador'].astype(str)
print(df_dados_com_meses_anos)
df_dados_com_meses_anos.set_index('Cód. do Trafo/Alimentador',inplace=True)
df_ordenado = df_dados_com_meses_anos.sort_values(by='Carregamento', ascending=False).head(40)
df_ordenado['Carregamento'] =df_ordenado['Carregamento'].round(2)
print(df_ordenado)
df_ordenado.info()
df_ordenado.reset_index(inplace=True)
print(df_ordenado.dtypes)
df_ordenado['Cor'] = df_ordenado['Carregamento'].apply(lambda x: 'Acima de 100%' if x > 100.0 else 'Menor que 100%')
fig3 = px.bar(df_ordenado,x='Cód. do Trafo/Alimentador', y='Carregamento', title='Gráfico de Carregamento', color='Cor')
fig3.update_traces(texttemplate='%{y}', textposition='outside')  # texttemplate define o formato do texto, textposition define a posição
fig3.update_layout(xaxis_title="Transformador",yaxis_title="Carregamento (%)",width=1480, height=680)
fig3.update_xaxes(tickfont=dict(size=12))
fig3.update_yaxes(range=[0, 160])


# Ordenar categorias pelo total descendente
st.plotly_chart(fig3)
########################################################################################################################
########################################## DIAGNÓSTICO ALIMENTADOR #####################################################
st.divider()

st.subheader('Diagnóstico Dos Alimentadores')
df_dados_com_meses_anos = df_maxima
df_dados_com_meses_anos = df_dados_com_meses_anos.loc[df_dados_com_meses_anos["Tipo"]=='Alimentador']
df_dados_com_meses_anos['Cód. do Trafo/Alimentador']=df_dados_com_meses_anos['Cód. do Trafo/Alimentador'].astype(str)
print(df_dados_com_meses_anos)
df_ordenado = df_dados_com_meses_anos.sort_values(by='Carregamento', ascending=False).head(40)
df_ordenado['Carregamento'] =df_ordenado['Carregamento'].round(2)
print(df_ordenado)
df_ordenado.info()
print(df_ordenado.columns)
df_ordenado.rename(columns={'Cód. do Trafo/Alimentador': 'Trafo_Alimentador'}, inplace=True)
print(df_ordenado.dtypes)
df_ordenado['Cor'] = df_ordenado['Carregamento'].apply(lambda x: 'Acima de 100%' if x > 100.0 else 'Menor que 100%')
fig4 = px.bar(df_ordenado, x='Trafo_Alimentador', y='Carregamento', title='Gráfico de Carregamento', color='Cor')
fig4.update_layout(xaxis_title="Alimentador",yaxis_title="Carregamento (%)",width=1480, height=680)
fig4.update_xaxes(tickfont=dict(size=12))
fig4.update_traces(texttemplate='%{y}', textposition='outside')
fig4.update_xaxes(tickfont=dict(family='Arial, bold'))
fig4.update_yaxes(range=[0, 200])


# Ordenar categorias pelo total descendente
st.plotly_chart(fig4)

######################################################################################################################

############################# FAZER DOWNLOAD DO ESTUDO #################################################################

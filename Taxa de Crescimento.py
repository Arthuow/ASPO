import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
################################# TAXA DE CRESCIMENTO #################################################################
df_maxima_2022 = pd.read_excel("Valores_maximos_P_meses_2022.xlsx", sheet_name="Potência Ativa Máxima")
df_maxima_2023 = pd.read_excel("Valores_maximos_P_meses.xlsx", sheet_name="Potência Ativa Máxima")
colunas_filtradas_2022 = ['Cód. do Trafo/Alimentador','Pot. Máxima']
df_maxima_2022=df_maxima_2022[colunas_filtradas_2022]
df_maxima_2022 = df_maxima_2022.rename(columns={'Pot. Máxima': 'Pot. Máxima 2022'})
colunas_filtradas_2023 = ['Descrição','Cód. de Ident','Cód. do Trafo/Alimentador','Tensão Prim','Tensão Sec. (kV)','Potencia Instalada','Pot. Máxima','Carregamento','Tipo']
df_maxima_2023 = df_maxima_2023.rename(columns={'Pot. Máxima': 'Pot. Máxima 2023'})
df = pd.merge(df_maxima_2023, df_maxima_2022, on="Cód. do Trafo/Alimentador",how="left")
print(df)
#df_ordendado=df['Descrição','Cód. de Ident','Cód. do Trafo/Alimentador','Tensão Prim','Tensão Sec. (kV)','Potencia Instalada','Pot. Máxima 2022']

df.loc[df['Pot. Máxima 2022'] == 0, 'Tx_crescimento'] = 0

df['Tx_crescimento'] = (df['Pot. Máxima 2023']/df['Pot. Máxima 2022']-1)*100
df['Tx_crescimento'] = df['Tx_crescimento'].round(2)
df['Tx_crescimento_mensal'] = (df['Tx_crescimento'])**(1/12)

#Janeiro 2023	Fevereiro 2023	Março 2023	Abril 2023	Maio 2023	Junho 2023	Julho 2023	Agosto 2023	Pot. Máxima 2023	Carregamento	Tipo

dir = r"C:\Users\Engeselt\Documents\GitHub\ASPO\Taxa de Crescimento.xlsx"
df.to_excel(dir, sheet_name="Taxa de Crescimento",engine='openpyxl', index=False)

########################################################################################################################





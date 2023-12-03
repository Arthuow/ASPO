#Projeto Energisa de Leitura de dados de Demanda
import datetime
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import matplotlib.pyplot as plt
import csv
import numpy as np
import openpyxl
import os
import zipfile

############################### Manipulação de arquivo em ZIP ##########################################################

caminho_zip = r"C:\Users\Engeselt\Documents\Arquivo de Demanda\diário"
pasta_destino = r"C:\Users\Engeselt\Documents\Arquivo de Demanda\diário"

############## LIMPA OS ARQUIVOS DO TIOP CSV ############################
print("Limpando arquivos antigos do tipo.csv")
for nome_arquivo in os.listdir(caminho_zip):

    if nome_arquivo.endswith('.csv'):  # Verifique se o arquivo tem a extensão .csv
        arquivo_path = os.path.join(caminho_zip, nome_arquivo)
        os.remove(arquivo_path)
print("Limpeza Concluída")
#######################################################################

############## EXTRAI OS ARQUIVOS DO TIPO .TXT ##########################
print("Extraindo arquivo do tipo .txt do ZIP")
for nome_arquivo_zip in os.listdir(caminho_zip):
    caminho_arquivo_zip = os.path.join(caminho_zip, nome_arquivo_zip)

    if nome_arquivo_zip.endswith('.zip'):
        with zipfile.ZipFile(caminho_arquivo_zip, 'r') as zip_ref:
            for nome_arquivo in zip_ref.namelist():
                if nome_arquivo.endswith('.txt'):
                    zip_ref.extract(nome_arquivo, pasta_destino)
                    print(f'Arquivo {nome_arquivo} extraído com sucesso para {pasta_destino}')
    else:
        print(f'O arquivo .zip {nome_arquivo_zip} não foi encontrado')
print("Extração Concluída")
#########################################################################

###################### Renomear arquivos .txt para .csv #################
print("RENOMEANDO ARQUIVOS .TXT PARA .CSV")
for nome_arquivo in os.listdir(pasta_destino):
    caminho_arquivo = os.path.join(pasta_destino, nome_arquivo)

    if nome_arquivo.endswith('.txt'):
        novo_nome_arquivo = os.path.splitext(nome_arquivo)[0] + '.csv'
        caminho_novo_arquivo = os.path.join(pasta_destino, novo_nome_arquivo)
        os.rename(caminho_arquivo, caminho_novo_arquivo)
        print(f'Arquivo {nome_arquivo} renomeado para {novo_nome_arquivo}')
print("PROCESSO CONCLUIDO")
########################################################################################################################

############################# Carregando os arquivos de Demanda ########################################################
print("CARREGANDO ARQUIVOS DE DEMANDA")
lista_dataFrame = []
lista_nome = []
diretorio = r"C:\Users\Engeselt\Documents\Arquivo de Demanda\diário"
for arq in os.listdir(diretorio):
    if arq.endswith(".csv"):# se o arquivo terminar com .csv
        nome_arquivo = "\\" + arq
        nome_df = pd.read_csv(diretorio+nome_arquivo,sep=";",encoding='latin-1')
        print("Nome do arquivo:",arq)
        lista_dataFrame.append(nome_df)


### Verifica se a coluna DATAS/Hora está presente nos arquivos##
contador = 1
df_principal = pd.DataFrame()
for i in lista_dataFrame[1:]:
    df_principal = pd.concat(lista_dataFrame)
    contador += 1
print("\nDados do df_principal:\n")
#df_principal=nome_df
print(df_principal)
###Apagar dados que não são do tipo DATATIME
print(df_principal.columns)
df_principal['DATA/Hora']=pd.to_datetime(df_principal['DATA/Hora'], errors="coerce")
df_principal.dropna(subset=['DATA/Hora'], inplace=True)
df_principal['DATA'] = df_principal['DATA/Hora'].dt.date
#Cria Coluna da Hora
df_principal['HORA'] = df_principal['DATA/Hora'].dt.hour
df_principal['DATA_HORA'] = pd.to_datetime(df_principal['DATA'].astype(str) + ' ' + df_principal['HORA'].astype(str),format = '%Y-%m-%d %H')
df_principal.set_index('DATA_HORA', inplace=True)

print("DF_PRINCIPAL: \n",df_principal)
df_principal.info()
print("PROCESSO CONCLUIDO")
##########################Agrupamento dos DADOS##########################

print("Limpando a Base de Dados")
df_principal.drop('DATA/Hora',axis=1, inplace=True)
colunas_numericas = df_principal.columns[1:-1]
df_principal[colunas_numericas] = df_principal[colunas_numericas].replace(',','.',regex=True).replace('','0')
df_principal[colunas_numericas] = df_principal[colunas_numericas].replace(',','.',regex=True).replace(' ','0')
df_principal[colunas_numericas] = df_principal[colunas_numericas].fillna('')
df_principal[colunas_numericas] = df_principal[colunas_numericas].replace(' ', '', regex=True)
df_principal[colunas_numericas] = df_principal[colunas_numericas].replace('', np.nan)
for coluna in df_principal.columns:
    df_principal[coluna] = pd.to_numeric(df_principal[coluna], errors='coerce').fillna(0).astype(int)
print('Inicio do agrupamento...\n')
df_agrupado_dia_hora_novo = df_principal.groupby('DATA_HORA').sum()
print("DF AGRUPADO DIA",df_agrupado_dia_hora_novo)
df_agrupado_dia_hora_novo.info()
print("DF_PRINCIPAL",df_principal)
df_principal.info()
#df_principal = df_principal.set_index('DATA_HORA')
print("Exportando a base de dados Agrupada")
df_agrupado_dia_hora_novo.to_csv("C:\\Users\\Engeselt\\Documents\\GitHub\\ASPO\\Medição Agrupada_diária.csv", sep=";", encoding='latin-1')
print("DF_PRINCIPAL AGRUPADO",df_agrupado_dia_hora_novo)

########################################################################################################################
##############################  Importação da base de dados existente ##################################################

print("importando a base de dados agrupada")
diretorio = r"C:\Users\Engeselt\Documents\GitHub\ASPO\Medição Agrupada.csv"
df_principal_2 = pd.read_csv(diretorio, sep=";", encoding='latin-1')
df_principal_2['DATA_HORA'] = pd.to_datetime(df_principal_2['DATA_HORA'])
df_principal_2['DATA_HORA'] = pd.to_datetime(df_principal_2['DATA_HORA'], format="%d/%m/%Y %H:%M", errors='coerce')
df_principal_2.set_index('DATA_HORA',inplace=True)
print("DF PRINCIPAL_2",df_principal_2)
df_principal_2.info()

########################################################################################################################

################################### Fazendo a mesclagem dos dados ######################################################
print("Fazendo mesclagem dos dados")
df_principal_2 = df_principal_2.reindex(df_agrupado_dia_hora_novo.index.union(df_principal_2.index))
df_principal_2.update(df_agrupado_dia_hora_novo,overwrite=True,errors='ignore')
#merged_df = df_principal_2.merge(df_agrupado_dia_hora_novo, how='outer', left_index=True, right_index=True)
#merged_df = pd.concat([df_principal_2, df_agrupado_dia_hora_novo])

# Merge the two DataFrames based on their index (DATA_HORA)
#df_principal_2 = df_principal_2.reindex(df_agrupado_dia_hora_novo.index.union(df_principal_2.index))
#merged_df = df_principal_2.merge(df_agrupado_dia_hora_novo, how='outer', left_index=True, right_index=True)
#merged_df = pd.concat([df_principal_2, df_agrupado_dia_hora_novo])
#print(merged_df)

# Update columns in df_principal_2 with aggregated values
#df_principal_2.update(merged_df.filter(like='_aggregated'))
#merged_df = merged_df.drop(merged_df.filter(like='_aggregated'), axis=1)  # Assign the result back to merged_df
print("mesclagem concluida")
########################################################################################################################
#print("Fazendo mesclagem dos dados")
# Renomear colunas em df_agrupado_dia_hora_novo para evitar sufixos _y
#df_agrupado_dia_hora_novo = df_agrupado_dia_hora_novo.add_suffix('_aggregated')

# Mesclar os dois DataFrames com base no índice (DATA_HORA)
#merged_df = df_principal_2.merge(df_agrupado_dia_hora_novo, how='outer', left_index=True, right_index=True)

# Atualizar colunas em df_principal_2 com valores agregados
#colunas_para_atualizar = ['DJ19 - SE QUATRO MARCOS-EAE', 'outra_coluna', 'mais_uma_coluna']
#for coluna in colunas_para_atualizar:
#    if coluna in df_principal_2.columns:
#        merged_df[coluna] = merged_df[coluna].combine_first(merged_df[coluna])

# Remover as colunas agregadas (_aggregated)
#merged_df = merged_df.drop(df_agrupado_dia_hora_novo.columns, axis=1)

#print(merged_df)
#print("mesclagem concluída")

#df_principal_2.index = pd.to_datetime(df_principal_2.index)
######################################### Filtro do dia #########################################################

current_date = datetime.datetime.now().date()
print(current_date)
#filtered_df = df_principal_2.loc[df_principal_2.index.date < current_date]
#filtered_df = df_principal_2[df_principal_2.index.dt.date < current_date]
filtered_df = df_principal_2[df_principal_2.index.to_series().dt.date < current_date]
############################ EXCLUINDO LINHAS DUPLIACADAS ##############################################################
#merged_df = merged_df[~merged_df.index.duplicated(keep='first')]
filtered_df = filtered_df[~filtered_df.index.duplicated(keep='first')]


################################## EXPORTANDO ARQUIVO FINAL ############################################################
print("inicio da exportação")
caminho_destino = r"C:\Users\Engeselt\Documents\GitHub\ASPO"
filtered_df.to_csv("C:\\Users\\Engeselt\\Documents\\GitHub\\ASPO\\Medição Agrupada.csv", sep=";", encoding='latin-1')
print("Arquivo Exportado")

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

############## LIMPA OS ARQUIVOS DO TIPO CSV ############################
print("Limpando arquivos antigos do tipo.csv")
for nome_arquivo in os.listdir(caminho_zip):
    if nome_arquivo.endswith('.csv'):
        arquivo_path = os.path.join(caminho_zip, nome_arquivo)
        os.remove(arquivo_path)
print("Limpeza Concluída")

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

############################# Carregando os arquivos de Demanda ########################################################
print("CARREGANDO ARQUIVOS DE DEMANDA")
lista_dataFrame = []
lista_nome = []
diretorio = r"C:\Users\Engeselt\Documents\Arquivo de Demanda\diário"
for arq in os.listdir(diretorio):
    if arq.endswith(".csv"):
        nome_arquivo = "\\" + arq
        nome_df = pd.read_csv(diretorio+nome_arquivo,sep=";",encoding='latin-1')
        print("Nome do arquivo:",arq)
        lista_dataFrame.append(nome_df)

### Verifica se a coluna DATA/Hora está presente nos arquivos##
contador = 1
df_principal = pd.DataFrame()
for i in lista_dataFrame[1:]:
    df_principal = pd.concat(lista_dataFrame)
    contador += 1
print("\nDados do df_principal:\n")
print(df_principal)

###Apagar dados que não são do tipo DATATIME
df_principal = df_principal[df_principal['DATA/Hora'] != 'TOTAL']
print(df_principal.columns)
df_principal['DATA/Hora']=pd.to_datetime(df_principal['DATA/Hora'], errors="coerce")
df_principal.dropna(subset=['DATA/Hora'], inplace=True)
df_principal['DATA'] = df_principal['DATA/Hora'].dt.date
df_principal['HORA'] = df_principal['DATA/Hora'].dt.hour
df_principal['HORA'] = df_principal['HORA'].apply(lambda x: f'{x:02}')
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

##################################### DEFININDO O PATAMAR DE CARGA #####################################################
def determinar_patamar_de_carga(data):
    mes = data.month
    dia_semana = data.weekday()
    hora = data.hour

    if 5 <= mes <= 8:  # maio a agosto
        if 0 <= dia_semana <= 4:  # Segunda a Sexta-feira
            if 1 <= hora <= 8:
                return "Leve"
            elif 9 <= hora <= 15:
                return "Média"
            elif 23 <= hora <= 24:
                return "Média"
            elif hora ==00:
                return "Média"
            else:
                return "Pesada"
        else:  # Sábado, Domingo e Feriado
            if 1 <= hora <= 18:
                return "Leve"
            else:
                return "Média"
    elif mes == 4 or mes == 9 or mes == 10:  # abril, setembro e outubro
        if 0 <= dia_semana <= 4:  # Segunda a Sexta-feira
            if 1 <= hora <= 8:
                return "Leve"
            elif 9 <= hora <= 14:
                return "Média"
            elif 23 <= hora <= 24:
                return "Média"
            elif hora ==00:
                return "Média"
            elif 19 <= hora <= 22:
                return "Leve"
            else:
                return "Pesada"
        else:  # Sábado, Domingo e Feriado
            if 1 <= hora <= 18:
                return "Leve"
            elif 19 <= hora <= 22:
                return "Média"
            else:
                return "Leve"
    else:  # novembro a março
        if 0 <= dia_semana <= 4:  # Segunda a Sexta-feira
            if 1 <= hora <= 8:
                return "Leve"
            elif 9 <= hora <= 13:
                return "Média"
            elif 23 <= hora <= 24:
                return "Média"
            elif hora ==00:
                return "Média"
            else:
                return "Pesada"
        else:  # Sábado, Domingo e Feriado
            if 1 <= hora <= 18:
                return "Leve"
            else:
                return "Média"

################################### ACRESCENTADO O COLUNA DE PATAMAR DE CARGA ##########################################
df_agrupado_dia_hora_novo['Patamar de Carga'] = df_agrupado_dia_hora_novo.index.to_series().apply(determinar_patamar_de_carga)

######################################### Filtro do dia #########################################################
current_date = datetime.datetime.now().date()
filtered_df = df_agrupado_dia_hora_novo[df_agrupado_dia_hora_novo.index.to_series().dt.date < current_date]
filtered_df = filtered_df[~filtered_df.index.duplicated(keep='first')]

################################## SALVANDO NO ACCESS ############################################################
print("Iniciando salvamento no Access")

# Caminho para o arquivo Access
access_file = r"C:\Users\Engeselt\Documents\GitHub\streamlit_demanda\demanda.accdb"

# String de conexão para o Access
conn_str = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    f"DBQ={access_file};"
)

try:
    # Conectar ao banco de dados
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Criar tabela se não existir
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS DemandaDiaria (
        DATA_HORA DATETIME PRIMARY KEY,
        Patamar_Carga TEXT,
        [Demanda] FLOAT
    )
    """
    cursor.execute(create_table_sql)
    
    # Preparar os dados para inserção
    for index, row in filtered_df.iterrows():
        # Converter o índice datetime para string no formato correto
        data_hora = index.strftime('%Y-%m-%d %H:%M:%S')
        patamar = row['Patamar de Carga']
        demanda = row['Demanda'] if 'Demanda' in row else 0
        
        # Inserir ou atualizar registro
        sql = """
        INSERT INTO DemandaDiaria (DATA_HORA, Patamar_Carga, [Demanda])
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE
        Patamar_Carga = VALUES(Patamar_Carga),
        [Demanda] = VALUES([Demanda])
        """
        cursor.execute(sql, (data_hora, patamar, demanda))
    
    # Commit das alterações
    conn.commit()
    print("Dados salvos com sucesso no Access!")
    
except Exception as e:
    print(f"Erro ao salvar no Access: {str(e)}")
    
finally:
    # Fechar conexão
    if 'conn' in locals():
        conn.close()

import math
import time
import datetime
import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore")
import matplotlib.pyplot as plt
import csv
import numpy as np
################# Definição de variáveis########################

max_date_time = None
################################################################
print("Importando Base de Dados Agrupada\n")
url_base = "Medição Agrupada.csv"
df_base = pd.read_csv(url_base, sep=";", encoding='latin-1')

# Converter a coluna DATA_HORA para datetime com tratamento de erros
try:
    # Primeiro tenta converter com o formato padrão
    df_base['DATA_HORA'] = pd.to_datetime(df_base['DATA_HORA'], format='%Y-%m-%d %H:%M:%S')
except ValueError:
    try:
        # Se falhar, tenta converter apenas a data
        df_base['DATA_HORA'] = pd.to_datetime(df_base['DATA_HORA'], format='%Y-%m-%d')
    except ValueError:
        # Se ainda falhar, tenta inferir o formato
        df_base['DATA_HORA'] = pd.to_datetime(df_base['DATA_HORA'], errors='coerce')

# Remover linhas com datas inválidas
df_base = df_base.dropna(subset=['DATA_HORA'])

# Definir DATA_HORA como índice
df_base = df_base.set_index(['DATA_HORA'])

df_base.info
print(df_base)
df_base['MES'] = df_base.index.month
df_base['ANO'] = df_base.index.year
df_base['MES'] = df_base['MES'].astype(int)
df_base['ANO'] = df_base['ANO'].astype(int)
print(df_base)
print("\nImportação Concluída")


url_atributos = "Tabela informativa.xlsx"
df_atributos_Dados = pd.read_excel(url_atributos, sheet_name = "Dados")
df_atributos_Dados.dropna(subset=['Codigo'], inplace=True)

##########DADOS TéCNICOS################

url_atributos = "Tabela informativa.xlsx"
df_atributos = pd.read_excel(url_atributos, sheet_name = "Dados Técnicos")
df_atributos.dropna(subset=['Cód. do Trafo/Alimentador'], inplace=True)
print(df_atributos)
df_dados_tecnicos=df_atributos
mes=1
anos = range(2024,2026)
meses = ['Janeiro','Fevereiro','Março','Abril','Maio','Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro']
colunas_meses_anos = [f'{mes} {ano}' for ano in anos for mes in meses]
df_meses_anos = pd.DataFrame(index=df_dados_tecnicos.index,columns=colunas_meses_anos)
df_dados_com_meses_anos = pd.concat([df_dados_tecnicos, df_meses_anos], axis=1)
df_dados_com_meses_anos_Q= pd.concat([df_dados_tecnicos, df_meses_anos], axis=1)
df_dados_com_meses_anos_S= pd.concat([df_dados_tecnicos, df_meses_anos], axis=1)
print("DF_DADOS_COM_MESES_ANOS")
print(df_dados_com_meses_anos)

df_valores_maximo_P = pd.DataFrame(columns = ['Cód. do Trafo/Alimentador'] + colunas_meses_anos)
for ano in range (2024,2026):
    df_filtrado_ano = df_base[df_base["ANO"]==ano]
    for mes in range (1,13):
         if mes in df_filtrado_ano['MES'].values:
            df_filtrado_mes = df_filtrado_ano[df_filtrado_ano["MES"]==mes]
            for selecao in df_atributos['Cód. do Trafo/Alimentador']:
                if selecao==0:
                    print('Valor de selecao é igual a 0. Continuando para a proxima iteração.')
                    continue
                EAE = str(selecao) + '-EAE'
                EAR = str(selecao) + '-EAR'
                ERE = str(selecao) + '-ERE'
                ERR = str(selecao) + '-ERR'
                try:
                    potencia_instalada = df_dados_tecnicos.loc[df_dados_tecnicos['Cód. do Trafo/Alimentador'] == selecao,'Potencia Instalada'].values[0]
                except IndexError:
                    potencia_instalada=0

                ########################
                filtro = df_atributos_Dados["Codigo"] == EAE
                indices_encontrados = df_atributos_Dados.loc[filtro, "Codigo"].index
                print('Indice encontrado:',indices_encontrados)

                if len(indices_encontrados) > 0:
                    indice_entrada = indices_encontrados[0]
                    descricao = df_atributos_Dados.loc[indice_entrada, 'descricao']
                else:
                    print("Chave não encontrada em df_atributos_Dados:", EAE)
                    continue

                #descricao = (df_atributos_Dados.loc[indice_entrada, 'descricao'])
                print(descricao)
                descricao = str(descricao)
                alimentador = descricao.replace("-EAE", "")
                indice_saida = df_atributos_Dados.loc[df_atributos_Dados['Codigo'] == EAR, 'Codigo'].index[0]
                descricao_saida = df_atributos_Dados.loc[indice_saida, 'descricao']
                descricao_saida = str(descricao_saida)
                print(descricao_saida)
                indice_entrada_Q = df_atributos_Dados.loc[df_atributos_Dados['Codigo'] == ERE, 'Codigo'].index[0]
                descricao_Q = (df_atributos_Dados.loc[indice_entrada_Q, 'descricao'])
                descricao_Q = str(descricao_Q)
                print(descricao_Q)
                indice_saida_Q = df_atributos_Dados.loc[df_atributos_Dados['Codigo'] == ERR, 'Codigo'].index[0]
                descricao_saida_Q = df_atributos_Dados.loc[indice_saida_Q, 'descricao']
                descricao_saida_Q = str(descricao_saida_Q)
                print(descricao_saida_Q)
                ########################################################################################################
                ###################################### BASE FILTRADA ###################################################
                df_base_filtrado = pd.DataFrame(df_filtrado_mes, columns=['DATA_HORA',descricao, descricao_saida, descricao_Q, descricao_saida_Q])
                df_base_filtrado.fillna(0.0)
                df_base_filtrado[descricao] = df_base_filtrado[descricao].astype(float)
                df_base_filtrado[descricao_saida] = df_base_filtrado[descricao_saida].astype(float)
                df_base_filtrado[descricao_Q] = df_base_filtrado[descricao_Q].astype(float)
                df_base_filtrado[descricao_saida_Q] = df_base_filtrado[descricao_saida_Q].astype(float)
                ########################################################################################################

                ####################################### CALCULO DOS PARAMETROS##########################################

                df_base_filtrado['P'] = df_base_filtrado[descricao] - df_base_filtrado[descricao_saida]
                df_base_filtrado['Q'] = df_base_filtrado[descricao_Q] - df_base_filtrado[descricao_saida_Q]
                df_base_filtrado['S'] = np.sqrt((df_base_filtrado['P'] ** 2) + (df_base_filtrado['Q'] ** 2))
                df_base_filtrado['fp'] = df_base_filtrado['P'] / df_base_filtrado['S']
                df_base_filtrado['ultrapassagem'] = df_base_filtrado['S'] / potencia_instalada >= 1.00
                df_base_filtrado['ultrapassagem'] = df_base_filtrado['ultrapassagem'].astype(int)
                df_base_filtrado["Ultrapassagem"] = df_base_filtrado.apply(lambda row: row["ultrapassagem"].sum(),
                                                                           axis=1)


                #######################################################################################################
                ####################### CALCULO DOS VALORES MÁXIMOS  E MINIMOS ########################################
                valor_maximo_P = max(df_base_filtrado['P'])
                valor_media_p = np.mean(df_base_filtrado['P'])
                valor_media_Q = np.mean(df_base_filtrado['Q'])
                valor_maximo_S = max(df_base_filtrado['S'])
                valor_maximo_S = round(float(valor_maximo_S), 2)
                valor_minimo_P = min(df_base_filtrado['P'])

                #######################################################################################################

                ###################################### CARREGAMENTO ##################################################
                carregamento = (valor_maximo_S / potencia_instalada) * 100
                carregamento_percentual = '{:.2f}%'.format(carregamento)

                ######################################################################################################
                #########################CALCULA VALOR MINIMO E O DESVIO PADRÃO SEM O ZERO ###########################

                dados_sem_zeros = np.array([x for x in df_base_filtrado['P'] if x != 0]).astype(float)
                if len(dados_sem_zeros) > 0:
                    valor_minimo_P_sem_zero = np.min(dados_sem_zeros)
                    desvio_padrao_P = np.nanstd(dados_sem_zeros)
                else:
                    valor_minimo_P_sem_zero = 0
                    desvio_padrao_P = 0
                #print("Dados sem o zero\n",dados_sem_zeros)
                #valor_minimo_P_sem_zero = min(dados_sem_zeros)
                print("Valor Minimo sem o zero:\n", valor_minimo_P_sem_zero)
                desvio_padrao_P = np.nanstd(df_base_filtrado['P'])
                print("desvio Padrão: ", desvio_padrao_P)
                ########################################################################################################

                ################################# CALCULO DO DESVIO PADRÃO #############################################
                minimo_desvio = round(valor_media_p - 2 * desvio_padrao_P, 0)
                maximo_desvio = round((valor_media_p + 3 * desvio_padrao_P), 0)
                print("Valor minimo por desvio padrão:", minimo_desvio)
                print("Valor maximo por desvio padrão:", maximo_desvio)
                ########################################################################################################
                ############################### CALCULO DO FATOR DE POTENCIA ###########################################

                fp = round(valor_media_p / (np.mean(df_base_filtrado['S'])), 2)


                ########################################################################################################
                ###########################  Potencia máxima considerada ###############################################
                base_filtrada = np.where(df_base_filtrado['P'] < valor_media_p + 3.0 * desvio_padrao_P, df_base_filtrado['P'], np.nan)
                valor_maximo_filtrado = np.nanmax(base_filtrada)

                if valor_maximo_P / valor_maximo_filtrado > 1.10:
                      valor_maximo_P = valor_maximo_filtrado
                else:
                   valor_maximo_P = valor_maximo_P

                # Obtenha a lista de índices onde 'P' é igual ao valor mínimo
                max_date_time_index = df_base_filtrado.index[df_base_filtrado['P'] == valor_maximo_P].tolist()

                print("Data e hora do valor máximo: ", max_date_time_index)

                # Certifique-se de que há pelo menos um índice encontrado
                if max_date_time_index and not pd.isna(max_date_time_index[0]):
                    # Pegue o primeiro índice encontrado (você pode ajustar conforme necessário)
                    primeiro_indice = max_date_time_index[0]

                    # Acesse diretamente o valor da coluna 'Q' correspondente ao índice mínimo de 'P'
                    valor_Q_correspondente = df_base_filtrado.at[primeiro_indice, 'Q']
                    print(f"Valor de 'Q' correspondente ao mínimo de 'P': {valor_Q_correspondente}")
                else:
                    print("Nenhum índice encontrado para o valor mínimo de 'P'")

                potencia_aparente_maxima = np.sqrt((valor_maximo_P ** 2) + (valor_Q_correspondente ** 2))


                ########################################################################################################
                if valor_maximo_P is not None:  # Verifica se um valor válido foi calculado
                    linha = df_atributos[df_atributos['Cód. do Trafo/Alimentador'] == selecao].index[0]
                    coluna_mes_ano = f'{meses[mes - 1]} {ano}'
                    df_dados_com_meses_anos.loc[linha, coluna_mes_ano] = valor_maximo_P


                ################################# POTENCIA REATIVA #####################################################
                if valor_Q_correspondente is not None:  # Verifica se um valor válido foi calculado
                    linha = df_atributos[df_atributos['Cód. do Trafo/Alimentador'] == selecao].index[0]
                    coluna_mes_ano = f'{meses[mes - 1]} {ano}'
                    df_dados_com_meses_anos_Q.loc[linha, coluna_mes_ano] =valor_Q_correspondente
                #######################################################################################################

                ################################# POTENCIA APARENTE #####################################################
                if valor_maximo_S is not None:  # Verifica se um valor válido foi calculado
                    linha = df_atributos[df_atributos['Cód. do Trafo/Alimentador'] == selecao].index[0]
                    coluna_mes_ano = f'{meses[mes - 1]} {ano}'
                    df_dados_com_meses_anos_S.loc[linha, coluna_mes_ano] = potencia_aparente_maxima
                #######################################################################################################


                ####potencia mínima considerada ####
                if valor_minimo_P < minimo_desvio:
                    valor_minimo_P = minimo_desvio
                else:
                    valor_minimo_P

                if selecao not in df_dados_tecnicos['Cód. do Trafo/Alimentador'].values:
                    print(f'O valor {selecao} não está presente em df_dados_tecnicos. Pulando para a próxima iteração.')
                    continue
            else:
                continue
            # linha = df_atributos[df_atributos['Cód. do Trafo/Alimentador'] == selecao].index[0]
            # coluna_mes_ano = f'{meses[mes - 1]} {ano}'
            # df_dados_com_meses_anos.loc[linha, coluna_mes_ano] = valor_maximo_P
    else:
        continue
########################################################################################################################
export_dir = "Exported_Data"
if not os.path.exists(export_dir):
    os.makedirs(export_dir)
#######################################################################################################################

################################## INSERINDO A INFORMAÇÃO DE CARREGAMENTO ###############################################
# Converta a coluna 'Potencia Instalada' para valores numéricos
df_dados_com_meses_anos['Potencia Instalada'] = pd.to_numeric(df_dados_com_meses_anos['Potencia Instalada'], errors='coerce')
excluir_colunas= ['Descrição', 'Cód. de Ident', 'Cód. do Trafo/Alimentador', 'Potencia Instalada','Tensão Prim','Tensão Sec. (kV)']
colunas_para_calcular = df_dados_com_meses_anos.drop(columns=excluir_colunas)
#df_dados_com_meses_anos['Pot. Máxima'] = df_dados_com_meses_anos.drop(excluir_colunas, axis=1).max(axis=1).round(2)
# Calcula o máximo de cada linha, preenchendo valores nulos com zero
colunas_numericas = colunas_para_calcular.apply(pd.to_numeric, errors='coerce')
df_dados_com_meses_anos['Pot. Máxima'] = colunas_numericas.max(axis=1).round(2)

# Divide o máximo de cada linha pela coluna 'Potencia Instalada' se for maior que zero

#COLUNAS NÃO NUMERICAS
non_numeric_columns = df_dados_com_meses_anos.select_dtypes(exclude=[np.number]).columns
print("Colunas não numéricas:", non_numeric_columns)
df_dados_com_meses_anos['Carregamento'] = (df_dados_com_meses_anos['Pot. Máxima'] / df_dados_com_meses_anos['Potencia Instalada'].where(df_dados_com_meses_anos['Potencia Instalada'] > 0, other=1))*100
df_dados_com_meses_anos['Carregamento'] =df_dados_com_meses_anos['Carregamento'].fillna(0).round(2)
df_dados_com_meses_anos.set_index('Cód. do Trafo/Alimentador')
df_dados_com_meses_anos["Ultrapassagem"] = sum(df_base_filtrado['ultrapassagem'])
print(df_dados_com_meses_anos)
########################################################################################################################

################################## INSERINDO A INFORMAÇÃO NA ABA POTENCIA REATIVA ######################################
df_dados_com_meses_anos_Q['Potencia Instalada'] = pd.to_numeric(df_dados_com_meses_anos_Q['Potencia Instalada'], errors='coerce')
excluir_colunas_Q = ['Descrição', 'Cód. de Ident', 'Cód. do Trafo/Alimentador', 'Potencia Instalada', 'Tensão Prim', 'Tensão Sec. (kV)']
colunas_para_calculo_Q = df_dados_com_meses_anos_Q.drop(columns=excluir_colunas_Q, errors='ignore')
colunas_numericas_Q = colunas_para_calculo_Q.apply(pd.to_numeric, errors='coerce')
df_dados_com_meses_anos_Q['Pot. Reativa'] = colunas_numericas_Q.max(axis=1).round(2)
# Divide o máximo de cada linha pela coluna 'Potencia Instalada' se for maior que zero

#COLUNAS NÃO NUMERICAS
non_numeric_columns = df_dados_com_meses_anos_Q.select_dtypes(exclude=[np.number]).columns
print("Colunas não numéricas:", non_numeric_columns)

df_dados_com_meses_anos_Q.set_index('Cód. do Trafo/Alimentador')

print(df_dados_com_meses_anos_Q)


################################## INSERINDO A INFORMAÇÃO DA ABA POTENCIA APARENTE #######################################

# Converta a coluna 'Potencia Instalada' para valores numéricos
df_dados_com_meses_anos_S['Potencia Instalada'] = pd.to_numeric(df_dados_com_meses_anos_S['Potencia Instalada'], errors='coerce')
excluir_colunas_S= ['Descrição', 'Cód. de Ident', 'Cód. do Trafo/Alimentador', 'Potencia Instalada','Tensão Prim','Tensão Sec. (kV)']
colunas_para_calcular_S = df_dados_com_meses_anos_S.drop(columns=excluir_colunas_S, errors='ignore')
colunas_numericas_S = colunas_para_calcular_S.apply(pd.to_numeric, errors='coerce')
df_dados_com_meses_anos_S['Pot. Aparente'] = colunas_numericas_S.max(axis=1).round(2)

# Calcular o carregamento usando a potência aparente máxima
df_dados_com_meses_anos_S['Carregamento'] = (df_dados_com_meses_anos_S['Pot. Aparente'] / df_dados_com_meses_anos_S['Potencia Instalada'].where(df_dados_com_meses_anos_S['Potencia Instalada'] > 0, other=1))*100
df_dados_com_meses_anos_S['Carregamento'] = df_dados_com_meses_anos_S['Carregamento'].fillna(0).round(2)

non_numeric_columns = df_dados_com_meses_anos_S.select_dtypes(exclude=[np.number]).columns
df_dados_com_meses_anos_S.set_index('Cód. do Trafo/Alimentador')
print(df_dados_com_meses_anos_S)

########################################################################################################################

########################################## CRIANDO A COLUNA TRANSFORMADOR OU ALIMENTADOR ###############################

def define_tipo(valor):
    valor_str = str(valor)
    if 'TR' in valor_str:
        return 'Transformador'
    else:
        return 'Alimentador'

# Aplicando a função e criando a nova coluna - POTÊNCIA ATIVA

df_dados_com_meses_anos['Tipo'] = df_dados_com_meses_anos['Cód. do Trafo/Alimentador'].apply(define_tipo)
df_dados_com_meses_anos.loc[df_dados_com_meses_anos['Tipo'] == 'Alimentador', 'Cód. do Trafo/Alimentador'] = 'AL-' + df_dados_com_meses_anos['Cód. do Trafo/Alimentador'].astype(str)
# Aplicando a função e criando a nova coluna - POTÊNCIA APARENTE
df_dados_com_meses_anos_S['Tipo'] = df_dados_com_meses_anos_S['Cód. do Trafo/Alimentador'].apply(define_tipo)
df_dados_com_meses_anos_S.loc[df_dados_com_meses_anos_S['Tipo'] == 'Alimentador', 'Cód. do Trafo/Alimentador'] = 'AL-' + df_dados_com_meses_anos_S['Cód. do Trafo/Alimentador'].astype(str)
# Agora, o DataFrame df_dados_com_meses_anos possui uma nova coluna 'Tipo' com as informações desejadas

########################################################################################################################
################################### EXPORTANDO O ARQUIVO ###############################################################

print('\nExportando para Excel...')
dir = "Demanda_Máxima_Não_Coincidente.xlsx"

# Primeira aba
with pd.ExcelWriter(dir, engine='openpyxl', mode='w') as writer:
    df_dados_com_meses_anos.to_excel(writer, sheet_name="Potência Ativa", index=False)
# Segunda aba
with pd.ExcelWriter(dir, engine='openpyxl', mode='a') as writer:
    df_dados_com_meses_anos_Q.to_excel(writer, sheet_name="Potência Reativa", index=False)
# Terceira aba
with pd.ExcelWriter(dir, engine='openpyxl', mode='a') as writer:
    df_dados_com_meses_anos_S.to_excel(writer, sheet_name="Potência Aparente", index=False)

print("Arquivo Exportado")
valores_maximos_P_meses = []

##################### EXPORTANDO FATOR DE POTENCIA ########################################

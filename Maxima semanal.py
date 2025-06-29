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
import duckdb
from pathlib import Path
import sqlite3

class maxima_semanal:
    def conectar_banco_duck():   
        try:
            # Conectar ao DuckDB com configurações otimizadas
            base_path = Path(__file__).resolve().parent
            db_path = base_path / "Medicoes.duckdb"
            conn = duckdb.connect(db_path, read_only=True)
            
            # Query otimizada para obter os dados já no formato desejado
            query = """
            SELECT 
                Data_Hora,
                ponto_medicao,
                valor
            FROM DemandaDiaria 
            ORDER BY Data_Hora
            """
            
            # Ler dados usando DuckDB com otimizações
            df_base_original = conn.execute(query).df()
            
            # Converter Data_Hora para datetime de forma otimizada
            df_base_original['Data_Hora'] = pd.to_datetime(df_base_original['Data_Hora'])
            df_base_original = df_base_original.rename(columns={'Data_Hora': 'DATA_HORA'})
            
            # Verificar se há dados
            if df_base_original.empty:
                raise ValueError("Nenhum dado encontrado na tabela DemandaDiaria")
            
            # Verificar colunas necessárias
            colunas_necessarias = ['DATA_HORA', 'ponto_medicao', 'valor']
            if not all(col in df_base_original.columns for col in colunas_necessarias):
                raise ValueError(f"Colunas necessárias não encontradas. Colunas presentes: {df_base_original.columns.tolist()}")
            
            # Pivotar os dados para ter os pontos de medição como colunas
            df_base_original = df_base_original.pivot_table(
                index='DATA_HORA',
                columns='ponto_medicao',
                values='valor',
                aggfunc='mean'
            ).reset_index()
            
            print("\nImportação do df_base Concluída")
            print(f"Colunas disponíveis: {df_base_original.columns.tolist()}")
            
        except Exception as e:
            print(f"Erro ao importar dados do DuckDB: {str(e)}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()

# Configurar o DataFrame base com otimizações
df_base = df_base_original.set_index(['DATA_HORA'])

# Calcular colunas de data de forma vetorizada
df_base['MES'] = df_base.index.month.astype('int32')
df_base['ANO'] = df_base.index.year.astype('int32')
df_base['SEMANA'] = df_base.index.isocalendar().week.astype('int32')

print("\nImportação do df_base Concluída")
print(f"Colunas disponíveis após processamento: {df_base.columns.tolist()}")

# Carregar tabelas informativas com otimizações
print('Importando tabela informativa:\n')
url_atributos = r"input\Tabela informativa.xlsx"
db_path = 'Dados.db'

def criar_banco_dados():
    """Cria o banco de dados SQLite e importa dados do Excel se necessário"""
    print("Conectando ao banco de dados...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Criar tabela de dados (sempre tenta criar, mesmo se já existir)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dados_medidores (
        codigo TEXT PRIMARY KEY,
        descricao TEXT,
        codigo_ident TEXT,
        potencia_instalada REAL,
        tensao_prim REAL,
        tensao_sec REAL
    )
    ''')
    
    # Limpar tabela existente
    cursor.execute("DELETE FROM dados_medidores")
    print("Tabela limpa para nova importação...")
    
    # Ler dados do Excel
    print("Importando dados do Excel...")
    df_atributos_Dados = pd.read_excel(
        url_atributos, 
        sheet_name="Dados",
        usecols=['Codigo', 'descricao', 'Cód. de Ident', 'Potencia Instalada', 'Tensão Prim', 'Tensão Sec. (kV)']
    )
    # Renomear colunas para minúsculas para manter consistência
    df_atributos_Dados.columns = ['codigo', 'descricao', 'codigo_ident', 'potencia_instalada', 'tensao_prim', 'tensao_sec']
    df_atributos_Dados.dropna(subset=['codigo'], inplace=True)
    
    # Inserir dados no SQLite usando INSERT OR REPLACE
    for _, row in df_atributos_Dados.iterrows():
        cursor.execute(
            """
            INSERT OR REPLACE INTO dados_medidores 
            (codigo, descricao, codigo_ident, potencia_instalada, tensao_prim, tensao_sec) 
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(row['codigo']), 
                str(row['descricao']),
                str(row['codigo_ident']) if pd.notna(row['codigo_ident']) else None,
                float(row['potencia_instalada']) if pd.notna(row['potencia_instalada']) else None,
                float(row['tensao_prim']) if pd.notna(row['tensao_prim']) else None,
                float(row['tensao_sec']) if pd.notna(row['tensao_sec']) else None
            )
        )
    
    conn.commit()
    print("Dados importados com sucesso!")
    conn.close()

def carregar_dados_tecnicos():
    """Carrega os dados técnicos do banco SQLite"""
    conn = sqlite3.connect(db_path)
    df_atributos_Dados = pd.read_sql_query("SELECT * FROM dados_medidores", conn)
    conn.close()
    
    if df_atributos_Dados.empty:
        raise ValueError("Nenhum dado encontrado na tabela 'dados_medidores'")
    
    print('Importação do df_atributos Concluída')
    print(f"Colunas disponíveis em df_atributos_Dados: {df_atributos_Dados.columns.tolist()}")
    
    return df_atributos_Dados

# Criar banco de dados e tabela
criar_banco_dados()

# Carregar dados do banco
df_atributos_Dados = carregar_dados_tecnicos()

# Carregar dados técnicos com otimizações
print('Importando tabela informativa para Dados Técnicos\n')
try:
    df_atributos = pd.read_excel(
        url_atributos, 
        sheet_name="Dados Técnicos"
    )
    
    # Verificar se há dados
    if df_atributos.empty:
        raise ValueError("Nenhum dado encontrado na aba 'Dados Técnicos' da tabela informativa")
    
    # Verificar colunas necessárias
    colunas_necessarias = ['Cód. do Trafo/Alimentador', 'Potencia Instalada', 'Tensão Prim', 'Tensão Sec. (kV)']
    colunas_faltantes = [col for col in colunas_necessarias if col not in df_atributos.columns]
    
    if colunas_faltantes:
        print(f"Aviso: As seguintes colunas não foram encontradas na aba 'Dados Técnicos': {colunas_faltantes}")
        print(f"Colunas disponíveis: {df_atributos.columns.tolist()}")
        raise ValueError(f"Colunas necessárias não encontradas: {colunas_faltantes}")
    
    # Selecionar apenas as colunas necessárias
    df_atributos = df_atributos[colunas_necessarias]
    df_atributos.dropna(subset=['Cód. do Trafo/Alimentador'], inplace=True)
    
    print('Importação do df_atributos Concluída')
    print(f"Colunas disponíveis em df_atributos: {df_atributos.columns.tolist()}")
    
except Exception as e:
    print(f"Erro ao importar dados técnicos: {str(e)}")
    raise

# Configurar variáveis de data de forma otimizada
data_atual = datetime.datetime.now()
semana_atual = data_atual.isocalendar()[1]
df_dados_tecnicos = df_atributos.copy()
meses = ['Janeiro','Fevereiro','Março','Abril','Maio','Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro']

# Criar DataFrames otimizados
colunas_meses_anos = [f'Semana {semana} {mes} {ano}' 
                     for ano in range(2025, 2026) 
                     for mes in meses 
                     for semana in range(1, 53)]

# Criar DataFrames com tipos de dados otimizados
df_meses_anos = pd.DataFrame(index=df_dados_tecnicos.index, columns=colunas_meses_anos, dtype='float32')
df_dados_com_meses_anos = pd.concat([df_dados_tecnicos, df_meses_anos], axis=1)
df_dados_com_meses_anos_Q = pd.concat([df_dados_tecnicos, df_meses_anos], axis=1)

print("DF_DADOS_COM_MESES_ANOS")
print(f"Colunas disponíveis: {df_dados_com_meses_anos.columns.tolist()}")
print("\nCriação concluída")

# Criar DataFrame para valores máximos com tipo otimizado
df_valores_maximo_P = pd.DataFrame(columns=['Cód. do Trafo/Alimentador'] + colunas_meses_anos, dtype='float32')

# Processar dados por ano e mês de forma otimizada
for ano in range(2025, 2026):
    df_filtrado_ano = df_base[df_base["ANO"] == ano]
    print(f"Calculando para o ano {ano}")
    
    for mes in range(1, 13):
        print(f"Calculando para o mês {mes}")
        if mes in df_filtrado_ano['MES'].values:
            df_filtrado_mes = df_filtrado_ano[df_filtrado_ano["MES"] == mes]
            
            # Calcular semanas do mês de forma otimizada
            primeiro_dia = pd.Timestamp(year=ano, month=mes, day=1)
            ultimo_dia = pd.Timestamp(year=ano, month=mes + 1, day=1) - pd.Timedelta(days=1)
            semanas = pd.date_range(start=primeiro_dia, end=ultimo_dia, freq='W-MON').isocalendar().week.unique()
            
            if len(semanas) > 0:
                max_semana = semanas.max()
                print(f"Número máximo da semana para {mes}/{ano}: {max_semana}")
                
                # Processar semanas em lotes para melhor performance
                for semana in range(1, max_semana + 1):
                    df_filtrado_semana = df_filtrado_mes[df_filtrado_mes["SEMANA"] == semana]
                    
                    if not df_filtrado_semana.empty:
                        print(f"Processando dados para {mes}/{ano} Semana {semana}")
                        
                        # Processar transformadores em lotes
                        for selecao in df_atributos['Cód. do Trafo/Alimentador']:
                            if selecao == 0:
                                continue
                                
                            # Preparar códigos de medição
                            EAE = f"{selecao}-EAE"
                            EAR = f"{selecao}-EAR"
                            ERE = f"{selecao}-ERE"
                            ERR = f"{selecao}-ERR"
                            
                            try:
                                potencia_instalada = df_dados_tecnicos.loc[
                                    df_dados_tecnicos['Cód. do Trafo/Alimentador'] == selecao,
                                    'Potencia Instalada'
                                ].values[0]
                            except IndexError:
                                potencia_instalada = 0
                                
                            # Verificar se o código existe
                            filtro = df_atributos_Dados["codigo"] == EAE
                            indices_encontrados = df_atributos_Dados.loc[filtro, "codigo"].index
                            
                            if len(indices_encontrados) == 0:
                                print(f"Chave não encontrada em df_atributos_Dados: {EAE}")
                                continue
                                
                            # Obter descrições de forma otimizada
                            indice_entrada = indices_encontrados[0]
                            descricao = str(df_atributos_Dados.loc[indice_entrada, 'descricao'])
                            alimentador = descricao.replace("-EAE", "")
                            
                            # Obter outras descrições
                            indice_saida = df_atributos_Dados.loc[df_atributos_Dados['codigo'] == EAR, 'codigo'].index[0]
                            descricao_saida = str(df_atributos_Dados.loc[indice_saida, 'descricao'])
                            
                            indice_entrada_Q = df_atributos_Dados.loc[df_atributos_Dados['codigo'] == ERE, 'codigo'].index[0]
                            descricao_Q = str(df_atributos_Dados.loc[indice_entrada_Q, 'descricao'])
                            
                            indice_saida_Q = df_atributos_Dados.loc[df_atributos_Dados['codigo'] == ERR, 'codigo'].index[0]
                            descricao_saida_Q = str(df_atributos_Dados.loc[indice_saida_Q, 'descricao'])
                            
                            # Processar dados de forma vetorizada
                            colunas_medicao = [descricao, descricao_saida, descricao_Q, descricao_saida_Q]
                            
                            # Verificar se todas as colunas existem no DataFrame
                            colunas_existentes = [col for col in colunas_medicao if col in df_filtrado_semana.columns]
                            
                            if len(colunas_existentes) != len(colunas_medicao):
                                colunas_faltantes = set(colunas_medicao) - set(colunas_existentes)
                                print(f"Colunas faltantes para {selecao}: {colunas_faltantes}")
                                continue
                            
                            try:
                                df_base_filtrado = df_filtrado_semana[colunas_medicao].fillna(0.0).astype('float32')
                                
                                # Calcular parâmetros de forma vetorizada
                                df_base_filtrado['P'] = df_base_filtrado[descricao] - df_base_filtrado[descricao_saida]
                                df_base_filtrado['Q'] = df_base_filtrado[descricao_Q] - df_base_filtrado[descricao_saida_Q]
                                df_base_filtrado['S'] = np.sqrt(df_base_filtrado['P'] ** 2 + df_base_filtrado['Q'] ** 2)
                                df_base_filtrado['fp'] = df_base_filtrado['P'] / df_base_filtrado['S']
                                df_base_filtrado['ultrapassagem'] = (df_base_filtrado['S'] / potencia_instalada >= 1.00).astype('int32')
                                
                                # Calcular valores máximos e médios de forma otimizada
                                valor_maximo_P = df_base_filtrado['P'].max()
                                valor_media_p = df_base_filtrado['P'].mean()
                                valor_media_Q = df_base_filtrado['Q'].mean()
                                valor_maximo_S = round(float(df_base_filtrado['S'].max()), 2)
                                valor_minimo_P = df_base_filtrado['P'].min()
                                
                                # Calcular carregamento
                                carregamento = (valor_maximo_S / potencia_instalada) * 100 if potencia_instalada > 0 else 0
                                
                                # Calcular estatísticas sem zeros
                                dados_sem_zeros = df_base_filtrado['P'][df_base_filtrado['P'] != 0]
                                if len(dados_sem_zeros) > 0:
                                    valor_minimo_P_sem_zero = dados_sem_zeros.min()
                                    desvio_padrao_P = dados_sem_zeros.std()
                                else:
                                    valor_minimo_P_sem_zero = 0
                                    desvio_padrao_P = 0
                                
                                # Calcular limites de desvio
                                minimo_desvio = round(valor_media_p - 3 * desvio_padrao_P, 0)
                                maximo_desvio = round(valor_media_p + 3 * desvio_padrao_P, 0)
                                
                                # Calcular fator de potência
                                fp = round(valor_media_p / df_base_filtrado['S'].mean(), 2)
                                
                                # Calcular potência máxima considerada
                                base_filtrada = np.where(
                                    df_base_filtrado['P'] < valor_media_p + 3.0 * desvio_padrao_P,
                                    df_base_filtrado['P'],
                                    np.nan
                                )
                                valor_maximo_filtrado = np.nanmax(base_filtrada)
                                
                                # Ajustar valor máximo se necessário
                                if valor_maximo_P / valor_maximo_filtrado > 1.10:
                                    valor_maximo_P = valor_maximo_filtrado
                                
                                # Calcular potência aparente máxima
                                potencia_aparente_maxima = np.sqrt(valor_maximo_P ** 2 + valor_media_Q ** 2)
                                
                                # Atualizar DataFrames de resultados
                                if valor_maximo_P is not None:
                                    linha = df_atributos[df_atributos['Cód. do Trafo/Alimentador'] == selecao].index[0]
                                    coluna_mes_ano = f'Semana {semana} {meses[mes - 1]} {ano}'
                                    df_dados_com_meses_anos.loc[linha, coluna_mes_ano] = valor_maximo_P
                                    df_dados_com_meses_anos_Q.loc[linha, coluna_mes_ano] = valor_media_Q
                                    
                            except Exception as e:
                                print(f"Erro ao processar dados para {selecao}: {str(e)}")
                                continue

########################################################################################################################
# Criar diretório de exportação se não existir
export_dir = "Exported_Data"
if not os.path.exists(export_dir):
    os.makedirs(export_dir)

################################## INSERINDO A INFORMAÇÃO DA ABA POTENCIA ATIVA ########################################
# Otimizar cálculos de potência máxima e carregamento
df_dados_com_meses_anos['Potencia Instalada'] = pd.to_numeric(df_dados_com_meses_anos['Potencia Instalada'], errors='coerce')

# Lista de colunas para excluir
excluir_colunas = ['Descrição', 'Cód. de Ident', 'Cód. do Trafo/Alimentador', 'Potencia Instalada', 'Tensão Prim', 'Tensão Sec. (kV)']

# Verificar quais colunas realmente existem no DataFrame
colunas_existentes = df_dados_com_meses_anos.columns.tolist()
colunas_para_excluir = [col for col in excluir_colunas if col in colunas_existentes]

if len(colunas_para_excluir) != len(excluir_colunas):
    colunas_faltantes = set(excluir_colunas) - set(colunas_para_excluir)
    print(f"Aviso: As seguintes colunas não foram encontradas e serão ignoradas: {colunas_faltantes}")

# Remover apenas as colunas que existem
colunas_para_calcular = df_dados_com_meses_anos.drop(columns=colunas_para_excluir, errors='ignore')

# Calcular potência máxima de forma vetorizada
colunas_numericas = colunas_para_calcular.apply(pd.to_numeric, errors='coerce')
df_dados_com_meses_anos['Pot. Máxima'] = colunas_numericas.max(axis=1).round(2)

# Calcular carregamento de forma vetorizada
df_dados_com_meses_anos['Carregamento'] = (
    df_dados_com_meses_anos['Pot. Máxima'] / 
    df_dados_com_meses_anos['Potencia Instalada'].where(df_dados_com_meses_anos['Potencia Instalada'] > 0, other=1)
) * 100
df_dados_com_meses_anos['Carregamento'] = df_dados_com_meses_anos['Carregamento'].fillna(0).round(2)

# Definir tipo de equipamento
def define_tipo(selecao):
    valor_str = str(selecao)
    if 'TR' in valor_str:
        return 'Transformador'
    elif 'LT' in valor_str:
        return 'LDAT'
    else:
        return 'Alimentador'

# Aplicar função de tipo de forma vetorizada
df_dados_com_meses_anos['Tipo'] = df_dados_com_meses_anos['Cód. do Trafo/Alimentador'].apply(define_tipo)
df_dados_com_meses_anos.loc[df_dados_com_meses_anos['Tipo'] == 'Alimentador', 'Cód. do Trafo/Alimentador'] = 'AL-' + df_dados_com_meses_anos['Cód. do Trafo/Alimentador'].astype(str)

################################### EXPORTANDO PARA SQLITE ###############################################################
print('\nExportando para SQLite...')

# Criar conexão com o banco SQLite
sqlite_path = r"C:\Users\Engeselt\Documents\GitHub\ASPO_2\Dados.db"
conn = sqlite3.connect(sqlite_path)
cursor = conn.cursor()

# Criar tabela otimizada
cursor.execute('''
CREATE TABLE IF NOT EXISTS demanda_maxima_semanal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    descricao TEXT,
    codigo_ident TEXT,
    codigo_trafo_alimentador TEXT,
    potencia_instalada REAL,
    tensao_prim REAL,
    tensao_sec REAL,
    potencia_maxima REAL,
    carregamento REAL,
    ultrapassagem INTEGER,
    tipo TEXT,
    semana INTEGER,
    mes INTEGER,
    ano INTEGER,
    potencia_reativa REAL
)
''')

# Preparar dados para inserção em lote
dados_para_inserir = []
for index, row in df_dados_com_meses_anos.iterrows():
    semana_cols = [col for col in df_dados_com_meses_anos.columns if 'Semana' in col]
    
    for col in semana_cols:
        parts = col.split()
        semana = int(parts[1])
        mes = meses.index(parts[2]) + 1
        ano = int(parts[3])
        
        potencia_reativa = df_dados_com_meses_anos_Q.loc[index, col] if col in df_dados_com_meses_anos_Q.columns else None
        
        dados_para_inserir.append((
            row['Descrição'],
            row['Cód. de Ident'],
            row['Cód. do Trafo/Alimentador'],
            row['Potencia Instalada'],
            row['Tensão Prim'],
            row['Tensão Sec. (kV)'],
            row[col] if pd.notna(row[col]) else None,
            row['Carregamento'],
            row['Ultrapassagem'],
            row['Tipo'],
            semana,
            mes,
            ano,
            potencia_reativa
        ))

# Inserir dados em lote
cursor.executemany('''
INSERT INTO demanda_maxima_semanal (
    descricao, codigo_ident, codigo_trafo_alimentador, 
    potencia_instalada, tensao_prim, tensao_sec,
    potencia_maxima, carregamento, ultrapassagem, tipo,
    semana, mes, ano, potencia_reativa
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', dados_para_inserir)

# Commit e fechar conexão
conn.commit()
conn.close()

print("Dados exportados para SQLite com sucesso!")

################################### EXPORTANDO O ARQUIVO EXCEL ###############################################################
print('\nExportando para Excel...')
dir = base_path / "Demanda_Máxima_Semana.xlsx"

# Exportar para Excel com otimizações
with pd.ExcelWriter(dir, engine='openpyxl') as writer:
    df_dados_com_meses_anos.to_excel(writer, sheet_name="Potência Ativa", index=False)
    df_dados_com_meses_anos_Q.to_excel(writer, sheet_name="Potência Reativa", index=False)

print("Arquivo Exportado")

if __name__ == "__main__":
    app = maxima_semanal()
    app.conectar_banco_duck()
import os
import zipfile
import tkinter as tk
from tkinter import filedialog
import logging
import pandas as pd
import sqlite3
import numpy as np
import datetime
from pathlib import Path

class AplicacaoProcessamento:
    def __init__(self):
        self.logger = self.setup_logger()
        base_path = Path(__file__).resolve().parent  # Pasta onde está o script atual
        self.caminho = base_path / "Medicoes.db"  

    def setup_logger(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        return logging.getLogger()
   
    def criar_banco_sqlite(self, caminho):
        if not os.path.exists(caminho):
            conn = sqlite3.connect(caminho)
            conn.close()
            self.logger.info(f"Banco SQLite criado em: {caminho}")
           
    def iniciar(self):
        self.logger.info("Iniciando processo de atualização da base de dados (SQLite)")
        self.logger.info("Selecione a pasta que contém os arquivos *.zip")
        lista_dfs = []
        root = tk.Tk()
        root.withdraw()
        pasta_zip = filedialog.askdirectory()
        self.logger.info(f"Pasta selecionada: {pasta_zip}")
        try:
            for arquivo_zip in os.listdir(pasta_zip):
                if arquivo_zip.endswith('.zip'):
                    caminho_zip = os.path.join(pasta_zip, arquivo_zip)
                    self.logger.info(f"📦 Processando arquivo: {caminho_zip}")
                    with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                        for arquivo in zip_ref.namelist():
                            if arquivo.endswith('.txt'):
                                self.logger.info(f"→ Lendo arquivo dentro do ZIP: {arquivo}")
                                with zip_ref.open(arquivo) as f:
                                    linhas = f.read().decode('latin1').splitlines()
                                    headers = linhas[0].strip().split(';')
                                    measurement_points = headers[1:]
                                    data_lines = linhas[1:]
                                    registros = []
                                    
                                    # Log do formato da primeira linha de dados
                                    if data_lines:
                                        self.logger.info(f"Formato da primeira linha de dados: {data_lines[0]}")
                                    
                                    for linha in data_lines:
                                        valores = linha.strip().split(';')
                                        if len(valores) - 1 != len(measurement_points):
                                            continue
                                        Data_Hora = valores[0]
                                        medidas = valores[1:]
                                        for ponto, valor in zip(measurement_points, medidas):
                                            registros.append([Data_Hora, ponto, valor])
                                    df = pd.DataFrame(registros, columns=['DATA/Hora', 'ponto_medicao', 'valor'])
                                    df = df[df['DATA/Hora'] != 'TOTAL']
                                    print('Antes da conversão\n')
                                    print(df.dtypes)
                                    df['valor'] = df['valor'].astype(str).str.replace(',', '.', regex=False).str.strip()
                                    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
                                    df = df.dropna(subset=['valor'])
                                    lista_dfs.append(df)
  
            if lista_dfs:
                df_final = pd.concat(lista_dfs, ignore_index=True)
                self.logger.info(f"✔️ Dados transformados. Formato final inicial: {df_final.shape}")
                self.logger.info(f"Primeiras linhas do DataFrame final inicial:\n{df_final.head()}")
                
                self.logger.info(f"Criando coluna hora para agrupamento")
                df_final['Data'] = pd.to_datetime(df_final['DATA/Hora']).dt.date
                df_final['Hora'] = pd.to_datetime(df_final['DATA/Hora']).dt.hour
                df_final['Hora'] = df_final['Hora'].apply(lambda x: f'{x:02d}:00:00')
                
                df_final['Data_Hora'] = pd.to_datetime(df_final['Data'].astype(str) + ' ' + df_final['Hora'])
                df_final.drop(columns=['Data', 'Hora'], inplace=True)
                
                # Converter Data_Hora para string no formato SQLite
                df_final['Data_Hora'] = df_final['Data_Hora'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                self.logger.info(f"Inicio do agrupamento dos dados pela hora: \n")
                df_final = df_final.groupby(['Data_Hora', 'ponto_medicao'])['valor'].sum().round(2)
                df_final = df_final.reset_index()
                
                # Ordenar os dados por Data_Hora
                df_final = df_final.sort_values('Data_Hora')
                
                self.logger.info(f"Após agrupamento e ordenação: {df_final.shape}")

                self.logger.info("Criando banco SQLite...")
                self.criar_banco_sqlite(self.caminho)
                
                # Enviar para SQLite
                self.enviar_para_sqlite(df_final, 'DemandaDiaria', self.caminho)
            else:
                self.logger.warning("⚠️ Nenhum arquivo .txt encontrado nos ZIPs.")
 
        except Exception as e:
            self.logger.error(f"❌ Erro durante o processamento: {str(e)}")
 
    def enviar_para_sqlite(self, df, tabela, caminho_banco='demanda.db'):
        try:
            self.logger.info("🔗 Conectando ao banco SQLite...")
            self.logger.info(f"DataFrame a ser inserido - Shape: {df.shape}")
            self.logger.info(f"Primeiras linhas do DataFrame:\n{df.head()}")
            
            conn = sqlite3.connect(caminho_banco)
            cursor = conn.cursor()
 
            # Verifica se a tabela já existe
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tabela}'")
            tabela_existe = cursor.fetchone() is not None
 
            if not tabela_existe:
                self.logger.info(f"📋 Criando tabela '{tabela}'...")
                cursor.execute(f'''
                    CREATE TABLE {tabela} (
                        Data_Hora DATETIME,
                        ponto_medicao TEXT,
                        valor REAL,
                        PRIMARY KEY (Data_Hora, ponto_medicao)
                    )
                ''')
                conn.commit()
            else:
                self.logger.info(f"📌 Tabela '{tabela}' já existe. Inserindo novos dados...")
 
            # Inserir todos os dados de uma vez
            registros = list(df.itertuples(index=False, name=None))
            total = len(registros)
 
            if total == 0:
                self.logger.warning("⚠️ Nenhum dado válido para inserir no SQLite. Verifique os arquivos de origem.")
                return
 
            # Usar INSERT OR REPLACE para lidar com duplicatas
            cursor.executemany(
                f'INSERT OR REPLACE INTO {tabela} (Data_Hora, ponto_medicao, valor) VALUES (?, ?, ?)',
                registros
            )
            conn.commit()
            self.logger.info(f"🎉 Inseridos {total} registros na tabela '{tabela}' com sucesso.")
 
            # Verificar dados duplicados
            cursor.execute(f'''
                SELECT Data_Hora, ponto_medicao, COUNT(*) as count
                FROM {tabela}
                GROUP BY Data_Hora, ponto_medicao
                HAVING count > 1
            ''')
            duplicatas = cursor.fetchall()
            if duplicatas:
                self.logger.warning(f"⚠️ Encontrados {len(duplicatas)} registros duplicados que foram substituídos.")
 
            cursor.close()
            conn.close()
 
            self.logger.info(f"🎉 Todos os dados foram inseridos na tabela '{tabela}' com sucesso.")
 
        except Exception as e:
            self.logger.error(f"❌ Erro ao enviar dados para o SQLite: {e}")
 
if __name__ == "__main__":
    app = AplicacaoProcessamento()
    app.iniciar()

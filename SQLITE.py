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
import re
import unidecode

class AplicacaoProcessamento:
    def __init__(self):
        self.logger = self.setup_logger()
        base_path = Path(__file__).resolve().parent
        self.caminho = base_path / "DataBase" / "Medicoes.db"

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
        root = tk.Tk()
        root.withdraw()
        pasta_zip = filedialog.askdirectory()
        self.logger.info(f"Pasta selecionada: {pasta_zip}")
        try:
            encontrou_txt = False
            for arquivo_zip in os.listdir(pasta_zip):
                if arquivo_zip.endswith('.zip'):
                    caminho_zip = os.path.join(pasta_zip, arquivo_zip)
                    self.logger.info(f"📦 Processando arquivo: {caminho_zip}")
                    with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                        for arquivo in zip_ref.namelist():
                            if arquivo.endswith('.txt'):
                                encontrou_txt = True
                                self.logger.info(f"→ Lendo arquivo dentro do ZIP: {arquivo}")
                                with zip_ref.open(arquivo) as f:
                                    linhas = f.read().decode('latin1').splitlines()
                                    if not linhas:
                                        self.logger.warning(f"Arquivo {arquivo} está vazio.")
                                        continue
                                    df = pd.DataFrame([linha.split(';') for linha in linhas])
                                    df.columns = df.iloc[0]
                                    df = df[1:]

                                    if 'DATA/Hora' in df.columns:
                                        df = df[df['DATA/Hora'] != 'TOTAL']
                                        df['Data'] = pd.to_datetime(df['DATA/Hora'], errors='coerce').dt.date
                                        df['Hora'] = pd.to_datetime(df['DATA/Hora'], errors='coerce').dt.hour
                                        df['Hora'] = df['Hora'].apply(lambda x: f'{x:02d}:00:00')
                                        df['Data_Hora'] = pd.to_datetime(df['Data'].astype(str) + ' ' + df['Hora'], errors='coerce')
                                        df.drop(columns=['Data', 'Hora', 'DATA/Hora'], inplace=True)

                                        colunas_numericas = [col for col in df.columns if col != 'Data_Hora']
                                        df[colunas_numericas] = df[colunas_numericas].replace(',', '.', regex=True).replace('', '0')
                                        df[colunas_numericas] = df[colunas_numericas].replace(' ', '0').fillna('')
                                        df[colunas_numericas] = df[colunas_numericas].replace('', np.nan)
                                        df[colunas_numericas] = df[colunas_numericas].apply(pd.to_numeric, errors='coerce').fillna(0)
                                        df['Data_Hora'] = df['Data_Hora'].dt.strftime('%Y-%m-%d %H:%M:%S')
                                        df = df.drop_duplicates(subset=['Data_Hora'] + list(colunas_numericas))
                                        df = df.sort_values('Data_Hora')
                                        self.logger.info(f"Inicio do agrupamento dos dados pela hora.")
                                        df = df.groupby(['Data_Hora']).sum().round(2).reset_index()
                                        df = df.loc[:, ~df.columns.duplicated()]

                                        # 🔁 Nome da tabela baseado no conteúdo entre parênteses do nome do TXT
                                        padrao_parenteses = re.findall(r'\(([^)]+)\)', arquivo)
                                        if padrao_parenteses:
                                            nome_tabela = padrao_parenteses[0].lower().replace(" ", "_").replace("-", "_")
                                            self.logger.info(f"Nome da tabela extraído dos parênteses: '{nome_tabela}'")
                                        else:
                                            # fallback: nome do arquivo sem extensão, sanitizado
                                            nome_tabela = os.path.splitext(os.path.basename(arquivo))[0].lower().replace(" ", "_").replace("-", "_")
                                            self.logger.warning(f"Nome entre parênteses não encontrado, usando nome do arquivo: '{nome_tabela}'")

                                        self.logger.info(f"Inserindo na tabela '{nome_tabela}' - shape: {df.shape}")
                                        self.enviar_para_sqlite(df, nome_tabela, caminho_banco=self.caminho)
                                    else:
                                        self.logger.warning("Coluna 'DATA/Hora' não encontrada no DataFrame. Pulando limpeza específica.")

            if not encontrou_txt:
                self.logger.warning("⚠️ Nenhum arquivo .txt encontrado nos ZIPs.")
        except Exception as e:
            self.logger.error(f"❌ Erro durante o processamento: {str(e)}")


    def enviar_para_sqlite(self, df, tabela, caminho_banco='Medicoes.db'):
        try:
            self.logger.info("🔗 Conectando ao banco SQLite...")
            self.logger.info(f"DataFrame a ser inserido - Shape: {df.shape}")
            self.logger.info(f"Primeiras linhas do DataFrame:\n{df.head()}")
            conn = sqlite3.connect(caminho_banco)
            cursor = conn.cursor()

            # Remove colunas duplicadas por normalização completa (acentos, case, espaços, hífens, pontos, etc)
            def normalize_col(col):
                return re.sub(r'[^a-zA-Z0-9]', '', unidecode.unidecode(str(col).strip().lower()))

            normalized_seen = set()
            cols_to_keep = []
            for col in df.columns:
                norm = normalize_col(col)
                if norm not in normalized_seen:
                    normalized_seen.add(norm)
                    cols_to_keep.append(col)
                else:
                    self.logger.warning(f"Coluna removida por duplicidade após normalização: {col}")
            df = df[cols_to_keep]

            # Padroniza nome da coluna de data/hora
            for col in df.columns:
                if col.strip().upper() in ["DATA_HORA", "DATA/HORA", "DATA-HORA"]:
                    df = df.rename(columns={col: "Data_Hora"})


            # Limita o número de colunas para 2000 (SQLite limit)
            max_colunas = 2000
            if len(df.columns) > max_colunas:
                self.logger.warning(f"⚠️ DataFrame possui {len(df.columns)} colunas. Cortando para 2000 devido ao limite do SQLite.")
                df = df.iloc[:, :max_colunas]

            # Detecta tipo de cada coluna
            tipos_colunas = {}
            for col in df.columns:
                if col == "Data_Hora":
                    tipos_colunas[col] = "DATETIME"
                elif pd.api.types.is_numeric_dtype(df[col]):
                    tipos_colunas[col] = "REAL"
                else:
                    tipos_colunas[col] = "TEXT"

            # Verifica se a tabela existe
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tabela}'")
            tabela_existe = cursor.fetchone() is not None

            if not tabela_existe:
                self.logger.info(f"📋 Tabela '{tabela}' não existe. Criando...")
                colunas_sql = ',\n    '.join([
                    f"[{col}] {tipos_colunas[col]}" + (" PRIMARY KEY" if col == "Data_Hora" else "") for col in df.columns
                ])
                cursor.execute(f"""
                    CREATE TABLE {tabela} (
                        {colunas_sql}
                    )
                """)
                conn.commit()
            else:
                self.logger.info(f"📌 Tabela '{tabela}' já existe. Inserindo novos dados...")
                # Adiciona colunas que faltam, ignorando colunas já existentes (case-insensitive, normalizado)
                cursor.execute(f"PRAGMA table_info({tabela})")
                existing_cols = [info[1] for info in cursor.fetchall()]
                existing_norm = {re.sub(r'[^a-zA-Z0-9]', '', unidecode.unidecode(str(c).strip().lower())) for c in existing_cols}
                for col in df.columns:
                    norm = re.sub(r'[^a-zA-Z0-9]', '', unidecode.unidecode(str(col).strip().lower()))
                    if norm not in existing_norm:
                        cursor.execute(f"ALTER TABLE {tabela} ADD COLUMN [{col}] {tipos_colunas[col]}")
                        existing_norm.add(norm)
                conn.commit()

            registros = list(df.itertuples(index=False, name=None))
            total = len(registros)

            if total == 0:
                self.logger.warning("⚠️ Nenhum dado válido para inserir no SQLite. Verifique os arquivos de origem.")
                cursor.close()
                conn.close()
                return

            # Monta a query dinamicamente para todas as colunas
            colunas_str = ', '.join([f'[{col}]' for col in df.columns])
            valores_str = ', '.join(['?' for _ in df.columns])
            try:
                cursor.executemany(
                    f'INSERT OR REPLACE INTO {tabela} ({colunas_str}) VALUES ({valores_str})',
                    registros
                )
                conn.commit()
                self.logger.info(f"🎉 Inseridos {total} registros na tabela '{tabela}' com sucesso.")
            except sqlite3.OperationalError as e:
                if 'too many columns' in str(e).lower():
                    self.logger.error(f"❌ Erro: Tabela '{tabela}' excedeu o limite de colunas do SQLite. Corte as colunas para <= 2000.")
                else:
                    self.logger.error(f"❌ Erro operacional ao inserir dados: {e}")
                cursor.close()
                conn.close()
                return

            # Verifica duplicatas por Data_Hora
            try:
                cursor.execute(f'''
                    SELECT Data_Hora, COUNT(*) as count
                    FROM {tabela}
                    GROUP BY Data_Hora
                    HAVING count > 1
                ''')
                duplicatas = cursor.fetchall()
                if duplicatas:
                    self.logger.warning(f"⚠️ Encontrados {len(duplicatas)} registros duplicados que foram substituídos.")
            except Exception as e:
                self.logger.warning(f"Aviso ao checar duplicatas: {e}")

            cursor.close()
            conn.close()

        except Exception as e:
            self.logger.error(f"❌ Erro ao enviar dados para o SQLite: {e}")

if __name__ == "__main__":
    app = AplicacaoProcessamento()
    app.iniciar()
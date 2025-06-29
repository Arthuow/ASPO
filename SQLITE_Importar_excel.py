import sqlite3 as sql
import pandas as pd
import os
from pathlib import Path

class ImportadorExcel:
    def __init__(self, db_path='Dados.db', excel_path='input/Tabela informativa.xlsx'):
        """Inicializa o importador com os caminhos do banco e do Excel"""
        self.db_path = db_path
        self.excel_path = excel_path
        
    def criar_banco(self):
        """Cria o banco de dados SQLite e suas tabelas"""
        try:
            # Remover banco de dados existente
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
                print("Banco de dados antigo removido.")
            
            conn = sql.connect(self.db_path)
            cursor = conn.cursor()
            
            # Criar tabela de dados técnicos
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS dados_tecnicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                descricao TEXT,
                codigo_ident TEXT,
                barra_anarede INTEGER,
                codigo_trafo_alimentador TEXT,
                tensao_prim REAL,
                tensao_sec REAL,
                potencia_instalada REAL
            )
            ''')
            
            conn.commit()
            print("Banco de dados criado com sucesso!")
            return conn
        except sql.Error as e:
            print(f"Erro ao criar banco de dados: {e}")
            return None
        finally:
            if 'conn' in locals():
                conn.close()
    
    def importar_excel(self):
        """Importa dados do Excel para o SQLite"""
        try:
            # Verificar se o arquivo Excel existe
            if not os.path.exists(self.excel_path):
                raise FileNotFoundError(f"Arquivo Excel não encontrado: {self.excel_path}")
            
            # Ler dados do Excel
            print(f"Lendo dados do Excel: {self.excel_path}")
            df = pd.read_excel(
                self.excel_path,
                sheet_name="Dados Técnicos",
                usecols=['Descrição', 'Cód. de Ident', 'Barra ANAREDE', 'Cód. do Trafo/Alimentador', 
                        'Tensão Prim', 'Tensão Sec. (kV)', 'Potencia Instalada']
            )
            print(df
                  )
            # Renomear colunas para minúsculas e sem espaços
            df.columns = ['descricao', 'codigo_ident', 'barra_anarede', 'codigo_trafo_alimentador', 
                         'tensao_prim', 'tensao_sec', 'potencia_instalada']
            
            # Remover linhas com valores nulos na descrição
            df.dropna(subset=['descricao'], inplace=True)
            
            # Conectar ao banco
            conn = sql.connect(self.db_path)
            cursor = conn.cursor()
            
            # Inserir dados
            for _, row in df.iterrows():
                try:
                    # Converter barra_anarede para inteiro, se possível
                    barra_anarede = int(row['barra_anarede']) if pd.notna(row['barra_anarede']) else None
                    
                    cursor.execute(
                        """
                        INSERT INTO dados_tecnicos 
                        (descricao, codigo_ident, barra_anarede, codigo_trafo_alimentador, 
                         tensao_prim, tensao_sec, potencia_instalada) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(row['descricao']),
                            str(row['codigo_ident']) if pd.notna(row['codigo_ident']) else None,
                            barra_anarede,
                            str(row['codigo_trafo_alimentador']) if pd.notna(row['codigo_trafo_alimentador']) else None,
                            float(row['tensao_prim']) if pd.notna(row['tensao_prim']) else None,
                            float(row['tensao_sec']) if pd.notna(row['tensao_sec']) else None,
                            float(row['potencia_instalada']) if pd.notna(row['potencia_instalada']) else None
                        )
                    )
                except ValueError as e:
                    print(f"Erro ao processar linha: {row['descricao']} - {e}")
                    continue
            
            conn.commit()
            print("Dados importados com sucesso!")
            
            # Verificar quantidade de registros
            cursor.execute("SELECT COUNT(*) FROM dados_tecnicos")
            count = cursor.fetchone()[0]
            print(f"Total de registros importados: {count}")
            
        except Exception as e:
            print(f"Erro ao importar dados: {e}")
        finally:
            if 'conn' in locals():
                conn.close()
    
    def verificar_dados(self):
        """Verifica os dados importados"""
        try:
            conn = sql.connect(self.db_path)
            cursor = conn.cursor()
            
            # Contar registros
            cursor.execute("SELECT COUNT(*) FROM dados_tecnicos")
            count = cursor.fetchone()[0]
            print(f"\nTotal de registros na tabela: {count}")
            
            # Mostrar primeiros registros
            cursor.execute("""
                SELECT descricao, codigo_ident, barra_anarede, codigo_trafo_alimentador, 
                       tensao_prim, tensao_sec, potencia_instalada 
                FROM dados_tecnicos LIMIT 5
            """)
            registros = cursor.fetchall()
            print("\nPrimeiros 5 registros:")
            for registro in registros:
                print(registro)
                
        except sql.Error as e:
            print(f"Erro ao verificar dados: {e}")
        finally:
            if 'conn' in locals():
                conn.close()

def main():
    # Criar instância do importador
    importador = ImportadorExcel()
    
    # Criar banco de dados
    importador.criar_banco()
    
    # Importar dados
    importador.importar_excel()
    
    # Verificar dados importados
    importador.verificar_dados()

if __name__ == "__main__":
    main()

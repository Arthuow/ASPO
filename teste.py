import sqlite3 as sql
from pathlib import Path
import logging
import pandas as pd
# Configurar logger global
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configurar diretório de logs

def testar_query():
    try:
        base_path = Path(__file__).resolve().parent
        db_path = base_path / "Cadastro" / "Dados.db"
        print("Caminho do banco:", db_path)
        print("Existe o arquivo?", db_path.exists())
        with sql.connect(str(db_path)) as conexao:
            cursor = conexao.cursor()
            cursor.execute("SELECT * FROM dados_tecnicos LIMIT 1")
            resultado = cursor.fetchone()
            print(resultado)
    except Exception as e:
        print(f"Erro ao conectar ou consultar o banco de dados: {e}")

testar_query()

def listar_colunas():
    base_path = Path(__file__).resolve().parent
    db_path = base_path / "Cadastro" / "Dados.db"
    with sql.connect(str(db_path)) as conexao:
        cursor = conexao.cursor()
        cursor.execute("PRAGMA table_info(dados_tecnicos)")
        for linha in cursor.fetchall():
            print(linha)
listar_colunas()

def medidores_hemera():
    try:
        logger.info("Iniciando a leitura dos dados técnicos")
        df_atributos = pd.read_excel('Tabela informativa.xlsx', sheet_name='Dados')
        print("Colunas disponíveis:", df_atributos.columns)
        # Ajustar o nome da coluna para 'descricao' conforme o arquivo Excel
        if 'descricao' not in df_atributos.columns:
            raise ValueError("A coluna 'descricao' não existe no arquivo Excel")
        df_atributos.dropna(subset=['Codigo'], inplace=True)
        logger.info("Dados técnicos lidos com sucesso")
        # enviar os dados para o banco de dados
        base_path = Path(__file__).resolve().parent
        db_path = base_path / "Cadastro" / "Dados.db"
        with sql.connect(str(db_path)) as conexao:
            #criar tabela se não existir
            conexao.execute("""
                CREATE TABLE IF NOT EXISTS medidores_hemera (
                    Codigo TEXT PRIMARY KEY,
                    Descricao TEXT
                )
            """)
            for _, row in df_atributos.iterrows():
                conexao.execute("""
                    INSERT OR IGNORE INTO medidores_hemera (Codigo, descricao)
                    VALUES (?, ?)
                """, (row['Codigo'], row['descricao']))
            conexao.commit()
            logger.info("Dados técnicos enviados para o banco de dados com sucesso")
    except Exception as e:
        logger.error(f"Erro ao enviar dados técnicos para o banco de dados: {e}")
medidores_hemera()
from pathlib import Path
import sqlite3
base_path = Path(__file__).resolve().parent
db_path = base_path / "DataBase" / "Medicoes.db"
conn = sqlite3.connect('Medicoes.db')
cursor = conn.cursor()

cursor.execute("PRAGMA table_info(demanda_sinop)")
colunas = [linha[1] for linha in cursor.fetchall()]
print("Quantidade de colunas:", len(colunas))
# Verificar duplicatas
duplicadas = set([col for col in colunas if colunas.count(col) > 1])

print("Colunas duplicadas:", duplicadas)



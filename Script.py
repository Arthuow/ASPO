import os
import subprocess

# Lista dos nomes dos arquivos que você deseja executar em sequência
diretorio_scripts = os.getcwd()
scripts_a_executar = ['ZIP.py','Demanda Máxima diária.py','Taxa de Crescimento.py','Demanda Máxima 2023.py']

# Diretório onde estão os scripts
diretorio_scripts = r'C:\Users\Engeselt\Documents\GitHub\ASPO'

# Loop para executar cada script
for script in scripts_a_executar:
    # Caminho completo do script
    caminho_completo = os.path.join(diretorio_scripts, script)

    # Comando para executar o script
    comando = ['python', caminho_completo]

    # Execute o script
    subprocess.run(comando)
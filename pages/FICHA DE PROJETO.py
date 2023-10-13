import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
import os

st.title("Bem Vindo a ASPO - EMT! 👋")
st.write("Assessoria de Planejamento e Orçamento ")
st.subheader("Sistema Desenvolvido para avaliar a Demanda Máxima dos Transformadores e Alimentadores da Energisa - MT")

st.divider()
df_maxima_3 = pd.read_excel("Valores_maximos_P_meses.xlsx", sheet_name="Potência Ativa Máxima")
diretorio_ficha = r'C:\Users\Engeselt\OneDrive - Energisa\Documentos - Planejamento e Orçamento EMT\Fichas de Projetos de novos alimentadores, SEs e LDATs\SEs'
equipamento_procurado = st.selectbox("Selecione o Cód. do Trafo/Alimentador", df_maxima_3['Cód. do Trafo/Alimentador'])

arquivos_encontrados=[]
for nome_arquivo in os.listdir(diretorio_ficha):
        arquivos_encontrados.append(nome_arquivo)
st.write("Arquivos encontrados:")
for arquivo in arquivos_encontrados:
    st.write(arquivo)
arquivo_selecionado = st.selectbox("Selecione um arquivo para download", arquivos_encontrados)

if arquivo_selecionado:
    caminho_completo = os.path.join(diretorio_ficha, arquivo_selecionado)
    st.write("Caminho completo:", caminho_completo)
    st.write("Tamanho do arquivo:", os.path.getsize(caminho_completo), "bytes")
    st.write("Tipo MIME:", st.file_uploader._get_mime_type(caminho_completo))
    st.download_button("Baixar arquivo", data=open(caminho_completo, "rb").read(), file_name=arquivo_selecionado)

def generate_downloadable_file():
    # Aqui você pode criar o conteúdo do arquivo que deseja disponibilizar para download
    file_content = "Conteúdo do arquivo para download."

    # Retorna o conteúdo do arquivo
    return file_content

# Botão de download
if st.button("Baixar Arquivo"):
    # Chama a função para gerar o conteúdo do arquivo
    file_content = generate_downloadable_file()

    # Define o nome do arquivo
    file_name = "arquivo_de_download.txt"

    # Abre uma janela de download com o conteúdo do arquivo
    st.download_button(label="Clique para baixar", data=file_content, file_name=file_name, key="download_button")


import streamlit as st

from streamlit_quill import st_quill

# Spawn a new Quill editor
content = st_quill()

# Display editor's content as you type
content

import streamlit as st

from streamlit_gallery.utils.readme import readme
from streamlit_quill import st_quill


def main():
    with readme("streamlit-quill", st_quill, __file__):
        c1, c2 = st.columns([3, 1])

        c2.subheader("Parameters")

        with c1:
            content = st_quill(
                placeholder="Write your text here",
                html=c2.checkbox("Return HTML", False),
                readonly=c2.checkbox("Read only", False),
                key="quill",
            )

            if content:
                st.subheader("Content")
                st.text(content)


if __name__ == "__main__":
    main()
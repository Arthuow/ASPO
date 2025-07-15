import sqlite3 as sql
import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

class Equipamentos:
    def exibir_interface(self):
        st.set_page_config(layout="wide")  # Configurar layout para ocupar melhor a tela

        st.title("Gerenciamento de Equipamentos")

        # Exibir tabela de equipamentos com AgGrid
        try:
            with sql.connect("Cadastro/Dados.db") as conexao:
                cursor = conexao.cursor()
                cursor.execute("PRAGMA table_info(dados_tecnicos)")
                colunas = [info[1] for info in cursor.fetchall()]  # Obter os nomes das colunas da tabela
                cursor.execute("SELECT * FROM dados_tecnicos")
                equipamentos = cursor.fetchall()
                if equipamentos:
                    df_equipamentos = pd.DataFrame(equipamentos, columns=colunas)  # Usar os nomes reais das colunas

                    # Exibir tabela com AgGrid
                    gb = GridOptionsBuilder.from_dataframe(df_equipamentos)
                    gb.configure_pagination(paginationAutoPageSize=True)
                    gb.configure_selection('single', use_checkbox=True)
                    grid_options = gb.build()

                    grid_response = AgGrid(
                        df_equipamentos,
                        gridOptions=grid_options,
                        update_mode=GridUpdateMode.SELECTION_CHANGED,
                        fit_columns_on_grid_load=True
                    )

                    # Abrir janela ao selecionar equipamento
                    if (
                        grid_response is not None and
                        'selected_rows' in grid_response and
                        isinstance(grid_response['selected_rows'], list) and
                        len(grid_response['selected_rows']) > 0 and
                        isinstance(grid_response['selected_rows'][0], dict)
                    ):
                        selecionado = grid_response['selected_rows'][0]
                        with st.expander("Editar/Excluir Equipamento", expanded=True):
                            st.write("Selecionado:", selecionado)
                            # Gerar campos editáveis para todas as colunas, exceto id
                            novos_valores = {}
                            for col in colunas:
                                if col == 'id':
                                    st.text_input(f"{col}", value=str(selecionado.get(col, '')), disabled=True, key=f"{col}_{selecionado.get('id','novo')}" )
                                else:
                                    novos_valores[col] = st.text_input(f"{col}", value=str(selecionado.get(col, '')) if selecionado.get(col) is not None else "", key=f"{col}_{selecionado.get('id','novo')}" )

                            col_mod, col_del = st.columns(2)
                            with col_mod:
                                if st.button("Modificar", key=f"modificar_{selecionado.get('id','novo')}"):
                                    try:
                                        with sql.connect("Cadastro/Dados.db") as conexao:
                                            cursor = conexao.cursor()
                                            set_clause = ", ".join([f"{c}=?" for c in novos_valores.keys()])
                                            values = list(novos_valores.values()) + [selecionado.get('id')]
                                            cursor.execute(f"UPDATE dados_tecnicos SET {set_clause} WHERE id=?", values)
                                            conexao.commit()
                                            st.success("Registro alterado com sucesso.")
                                    except sql.Error as e:
                                        st.error(f"Erro ao alterar registro: {e}")
                            with col_del:
                                if st.button("Excluir", key=f"excluir_{selecionado.get('id','novo')}"):
                                    try:
                                        with sql.connect("Cadastro/Dados.db") as conexao:
                                            cursor = conexao.cursor()
                                            cursor.execute("DELETE FROM dados_tecnicos WHERE id=?", (selecionado.get('id'),))
                                            conexao.commit()
                                            st.success("Registro excluído com sucesso.")
                                    except sql.Error as e:
                                        st.error(f"Erro ao excluir registro: {e}")
                else:
                    st.warning("Nenhum equipamento encontrado.")
        except sql.Error as e:
            st.error(f"Erro ao carregar equipamentos: {e}")

if __name__ == "__main__":
    app = Equipamentos()
    app.exibir_interface()
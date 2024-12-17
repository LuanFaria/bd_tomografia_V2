from db_connection import DatabaseManager
from insert_bd import inserir_dados
from excel_bd import CreateBdAgroMerge  # Importação da classe responsável pelo merge
import pandas as pd

if __name__ == "__main__":
    try:
        clients_folder = "C:/TOMOGRAFIA"
        output_file = "C:/Users/luan.faria/Desktop/cod_luan/cod/SIGMA/cod/codigo_banco_tomo/teste_2.xlsx"
        selected_client_ids = [111]

        db_config = {
            "dbname": "postgis_34_sample",
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": "5432"
        }

        db_manager = DatabaseManager(db_config)

        # Excluir linhas existentes
        db_manager.delete_rows_by_client_ids("public", "bd_tomografia", selected_client_ids)

        # Gerar dados consolidados no Excel
        merger = CreateBdAgroMerge(
            output_file=output_file,
            clients_folder=clients_folder,
            export_excel_file=True,
            selected_client_ids=selected_client_ids
        )
        merged_data = merger.merge_clients_bd_agro_data(db_manager=db_manager)
        print("Dados mesclados gerados com sucesso.")

        # Carregar os dados do Excel e inserir no banco de dados
        df = pd.read_excel(output_file)
        inserir_dados(df, "bd_tomografia", db_manager)

        print("Dados inseridos com sucesso!")

    except Exception as e:
        print(f"Erro: {e}")

    finally:
        # Fechar a conexão ao final da execução
        db_manager.close_connection()

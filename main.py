from excel_bd import CreateBdAgroMerge  # Importando a classe que foi ajustada para salvar em Excel
from db_connection import DatabaseManager
import pandas as pd
from insert_bd import formatar_valor, inserir_dados

if __name__ == "__main__":
    clients_folder = "C:/TOMOGRAFIA"
    output_file = "C:/Users/luan.faria/Desktop/cod_luan/cod/SIGMA/cod/codigo_banco_tomo/teste_2.xlsx"  # Alterando para .xlsx

    selected_client_ids = [111,2]  # IDs dos clientes que você quer selecionar

    # Configuração e acesso ao banco de dados
    db_config = {
        "dbname": "postgis_34_sample",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": "5432"
    }

    db_manager = DatabaseManager(db_config)

    # Excluir linhas com os IDs fornecidos na tabela bd_tomografia
    db_manager.delete_rows_by_client_ids("public", "bd_tomografia", selected_client_ids)

    # Gerar o Excel e carregar os dados
    merger = CreateBdAgroMerge(
        output_file=output_file,
        clients_folder=clients_folder,
        export_excel_file=True,  # Modificado para True, pois agora é Excel
        selected_client_ids=selected_client_ids
    )

    # Realiza o merge dos dados
    merged_data = merger.merge_clients_bd_agro_data(db_manager=db_manager)

    # Exibe os dados resultantes
    print(merged_data)

    # Define os tipos de dados para reinserção no banco
    data_types = {
        'client_id': 'integer', 'client_name': 'string', 'CHAVE': 'string',
        'SAFRA': 'integer', 'OBJETIVO': 'string', 'cliente': 'string',
        'TP_PROP': 'string', 'FAZENDA': 'string', 'SETOR': 'string',
        'SECAO': 'string', 'BLOCO': 'string', 'PIVO': 'string',
        'DESC_FAZ': 'string', 'TALHAO': 'string', 'VARIEDADE': 'string',
        'MATURACAO': 'string', 'AMBIENTE': 'string', 'ESTAGIO': 'string',
        'GRUPO_DASH': 'string', 'GRUPO_NDVI': 'string',
        'NMRO_CORTE': 'float', 'TAH': 'float', 'TPH': 'float',
        'DESC_CANA': 'string', 'AREA_BD': 'float', 'A_EST_MOAGEM': 'float',
        'A_COLHIDA': 'float', 'A_EST_MUDA': 'float', 'A_MUDA': 'float',
        'TCH_EST': 'float', 'TC_EST': 'float', 'TCH_REST': 'float',
        'TC_REST': 'float', 'TCH_REAL': 'float', 'TC_REAL': 'float',
        'DT_CORTE': 'date', 'DT_ULT_CORTE': 'date', 'DT_PLANTIO': 'date',
        'IDADE_CORTE': 'float', 'ATR': 'float', 'ATR_EST': 'float',
        'IRRIGACAO': 'string', 'grupo': 'string'
    }

    df = pd.read_excel(output_file)

    # Inserir os dados no banco
    inserir_dados(df, "bd_tomografia")

import os
import pandas as pd
import numpy as np
from datetime import datetime
from db_connection import DatabaseManager

class CreateBdAgroMerge:
    """
    Classe para mesclar e exportar dados BD_AGRO de clientes selecionados.
    """

    def __init__(self, output_file, clients_folder, export_excel_file, selected_client_ids):
        self.output_file = output_file
        self.clients_folder = clients_folder
        self.export_excel_file = export_excel_file
        self.selected_client_ids = selected_client_ids

        self.list_clients_to_remove = [
            '98', '99', '126', '127', '133', '134', 
            '137', '139', '140', '141', '148', '149', 
            '150', '151', '152', '154', '155', 
            '999'
        ]

    def merge_clients_bd_agro_data(self, db_manager=None):
        """
        Realiza o merge dos dados BD_AGRO para os IDs de clientes selecionados.
        """
        merged_bd_agro = pd.DataFrame()
        clients_bd_agro_files = self.__get_all_clients_bd_agro()

        # Consulta os nomes dos clientes e as informações "cliente" no banco
        client_info_map = {}
        if db_manager:
            client_info_map = self.fetch_client_info(db_manager)

        for client_bd_agro in clients_bd_agro_files:
            client_id = int(client_bd_agro['client_id'])

            if client_id not in self.selected_client_ids:
                continue

            # Preenche os dados do cliente
            client_name = client_info_map.get(client_id, {}).get('nome_cliente', "Desconhecido")
            cliente = client_info_map.get(client_id, {}).get('cliente', "Desconhecido")

            print(f'Processando dados do cliente: {client_name}')
            bd_agro = pd.read_excel(client_bd_agro['bd_agro_file'])

            bd_agro['client_id'] = client_id
            bd_agro['client_name'] = client_name  # Nome do cliente
            bd_agro['cliente'] = cliente          # Informação "cliente" do banco
            bd_agro['tc_est_colheita'] = np.where(bd_agro['TCH_REAL'] > 0, bd_agro['TC_EST'], 0)

            # Formatação de datas para o formato correto do banco de dados
            bd_agro['DT_PLANTIO'] = self.formatar_data(bd_agro['DT_PLANTIO'])
            bd_agro['DT_ULT_CORTE'] = self.formatar_data(bd_agro['DT_ULT_CORTE'])
            bd_agro['DT_CORTE'] = self.formatar_data(bd_agro['DT_CORTE'])

            merged_bd_agro = pd.concat([merged_bd_agro, bd_agro], ignore_index=True)

        if db_manager:
            merged_bd_agro = self.add_group_to_excel(db_manager, merged_bd_agro, self.selected_client_ids)

        if self.export_excel_file:
            # Salvando o DataFrame diretamente como um arquivo Excel
            merged_bd_agro.to_excel(self.output_file, index=False)
            print(f"Excel exportado - OK!: {self.output_file}")

        return merged_bd_agro

    def fetch_client_info(self, db_manager):
        """
        Busca os nomes dos clientes e a coluna 'cliente' no banco de dados.
        Retorna um dicionário {client_id: {"nome_cliente": nome_cliente, "cliente": cliente}}.
        """
        conn = db_manager.get_connection()
        client_info_map = {}
        try:
            with conn.cursor() as cursor:
                query = "SELECT id, nome_cliente, cliente FROM clientes"
                cursor.execute(query)
                results = cursor.fetchall()

                # Cria o dicionário de mapeamento
                client_info_map = {
                    row[0]: {"nome_cliente": row[1], "cliente": row[2]} for row in results
                }
        finally:
            conn.close()

        return client_info_map

    def __get_all_clients_bd_agro(self):
        """
        Retorna informações de todos os arquivos BD_AGRO dos clientes.
        """
        clients_bd_agro_files = []
        clients_bd_agro_folders = self.__get_all_clients_bd_agro_folder()

        for folder in clients_bd_agro_folders:
            file_info = self.__find_bd_agro_file(folder)
            if file_info:
                clients_bd_agro_files.append(file_info)

        return clients_bd_agro_files

    def __find_bd_agro_file(self, client_folder):
        folder_name = os.path.basename(client_folder)
        client_id, client_name = folder_name.split('_')[0], folder_name.split('_')[1]

        bd_agro_path = os.path.join(client_folder, "2_bd_agro")
        if os.path.exists(bd_agro_path):
            for file in os.listdir(bd_agro_path):
                if file.startswith('BD_AGRO_') and file.endswith('.xlsx'):
                    return {"client_id": client_id, "client_name": client_name, "bd_agro_file": os.path.join(bd_agro_path, file)}

        return {}

    def __get_all_clients_bd_agro_folder(self):
        return [
            os.path.join(self.clients_folder, folder)
            for folder in os.listdir(self.clients_folder)
            if folder.split('_')[0].isdigit() and folder.split('_')[0] not in self.list_clients_to_remove
        ]

    def add_group_to_excel(self, db_manager, excel_data, client_ids):
        """
        Adiciona informações do grupo ao Excel baseado nos IDs dos clientes.
        """
        conn = db_manager.get_connection()
        try:
            with conn.cursor() as cursor:
                query = """
                SELECT c.id AS client_id, g.nome AS grupo_nome
                FROM clientes c
                INNER JOIN cliente_grupo g ON c.grupo_id = g.id
                WHERE c.id = ANY(%s)
                """
                cursor.execute(query, (client_ids,))
                group_data = cursor.fetchall()
                group_map = {row[0]: row[1] for row in group_data}
                # Garante que o client_id está no tipo correto
                excel_data['client_id'] = excel_data['client_id'].astype(int)
                # Mapeamento dos grupos
                if not group_map:
                    print("Aviso: Nenhum dado de grupo encontrado. Preenchendo com 'Sem Grupo'.")
                    excel_data['grupo'] = "Sem Grupo"
                else:
                    # Faz o mapeamento
                    excel_data['grupo'] = excel_data['client_id'].map(group_map)

                    # Preenche os valores nulos com "Sem Grupo"
                    excel_data['grupo'] = excel_data['grupo'].fillna("Sem Grupo")
        finally:
            conn.close()
        return excel_data

    def formatar_data(self, coluna):
        """
        Converte as datas para o formato YYYY-MM-DD HH:MM:SS para o banco de dados.
        Se a data for inválida (ex: 1900-01-01), substitui por '1900-01-01 00:00:00'.
        """
        return pd.to_datetime(coluna, errors='coerce').apply(
            lambda x: self.validar_data(x)  # Aplica a validação
        )

    def validar_data(self, data):
        """
        Valida se a data é uma data válida.
        Se a data for inválida (como 1900-01-01 ou NaT), substitui por '1900-01-01 00:00:00'.
        """
        if pd.isnull(data) or data.year == 1900:
            #print(f"Data inválida detectada: {data}")
            return '1900-01-01 00:00:00'  # Substitui por '1900-01-01 00:00:00'
        return data.strftime('%Y-%m-%d %H:%M:%S')  # Retorna a data no formato necessári

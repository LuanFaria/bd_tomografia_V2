import psycopg2
from psycopg2.extras import execute_values

class DatabaseManager:
    """
    Classe para gerenciar a conexão e operações no banco de dados PostgreSQL.
    """

    _connection = None  # Variável estática para armazenar a conexão única

    def __init__(self, config):
        self.config = config

    def get_connection(self):
        """
        Retorna uma conexão única ao banco de dados.
        """
        if DatabaseManager._connection is None or DatabaseManager._connection.closed:
            try:
                DatabaseManager._connection = psycopg2.connect(**self.config)
            except Exception as e:
                print(f"Erro ao conectar ao banco de dados: {e}")
                raise
        return DatabaseManager._connection

    def close_connection(self):
        """
        Fecha a conexão com o banco de dados, se ela estiver aberta.
        """
        if DatabaseManager._connection and not DatabaseManager._connection.closed:
            DatabaseManager._connection.close()
            print("Conexão com o banco de dados fechada.")

    def delete_rows_by_client_ids(self, schema, table, client_ids):
        """
        Exclui linhas na tabela que correspondem aos IDs fornecidos.

        Parameters:
        - schema (str): Schema do banco de dados.
        - table (str): Nome da tabela.
        - client_ids (list): IDs dos clientes a serem excluídos.
        """
        query = f"DELETE FROM {schema}.{table} WHERE client_id = ANY(%s)"
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, (client_ids,))
                print(f"{cursor.rowcount} linhas excluídas da tabela {table}.")
            conn.commit()
        except Exception as e:
            print(f"Erro ao excluir linhas: {e}")

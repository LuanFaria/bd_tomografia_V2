import psycopg2
from psycopg2.extras import execute_values

class DatabaseManager:
    """
    Classe para gerenciar a conexão e operações no banco de dados PostgreSQL.
    """

    def __init__(self, config):
        """
        Inicializa a classe DatabaseManager.

        Parameters:
        - config (dict): Configuração do banco de dados (dbname, user, password, host, port).
        """
        self.config = config

    def connect(self):
        """
        Conecta ao banco de dados PostgreSQL.
        """
        try:
            conn = psycopg2.connect(**self.config)
            return conn
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")
            raise

    def delete_rows_by_client_ids(self, schema, table, client_ids):
        """
        Exclui linhas na tabela que correspondem aos IDs fornecidos.

        Parameters:
        - schema (str): Schema do banco de dados.
        - table (str): Nome da tabela.
        - client_ids (list): Lista de IDs de clientes a serem excluídos.
        """
        query = f"DELETE FROM {schema}.{table} WHERE client_id = ANY(%s)"
        try:
            conn = self.connect()
            with conn.cursor() as cursor:
                cursor.execute(query, (client_ids,))
                print(f"{cursor.rowcount} linhas excluídas da tabela {table}.")
            conn.commit()
        except Exception as e:
            print(f"Erro ao excluir linhas: {e}")
        finally:
            conn.close()
    
   
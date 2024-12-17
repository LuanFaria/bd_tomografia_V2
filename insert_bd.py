import pandas as pd
import psycopg2
from datetime import datetime
import numpy as np  # Importar para trabalhar com NaN
from db_connection import DatabaseManager

# Configurações do banco de dados
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgis_34_sample"
DB_USER = "postgres"
DB_PASSWORD = "postgres"


# Caminho do arquivo Excel
excel_file = "teste_2.xlsx"  # Altere para o caminho do seu arquivo Excel

# Define os tipos de dados para reinserção no banco
data_types = {
    'client_id': 'integer', 'client_name': 'string', 'CHAVE': 'string',
    'SAFRA': 'integer', 'OBJETIVO': 'string', 'DT_CORTE': 'date',
    'DT_ULT_CORTE': 'date', 'DT_PLANTIO': 'date',
    # Adicione outros campos e tipos aqui
}

# Função para conectar ao banco
def conectar_banco():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

# Função para formatar valores com base no tipo de dado
def formatar_valor(valor, tipo):
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):  # Verifica se o valor é NaN
        return None
    try:
        if tipo == 'integer':
            return int(valor)
        elif tipo == 'float':
            return float(valor)
        elif tipo == 'date':
            # Verifica se o valor é negativo
            if isinstance(valor, int) or (isinstance(valor, str) and valor.isdigit()):
                timestamp = int(valor)
                if timestamp < 0:
                    return "1900-01-01"  # Retorna a data padrão para valores negativos
                else:
                    return datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
            else:
                return None  # Ignorar valores inválidos para colunas de data
        elif tipo == 'string':
            return str(valor)
    except Exception as e:
        print(f"Erro ao formatar o valor '{valor}' do tipo '{tipo}': {e}")
        return None
    return valor

# Função para inserir os dados no banco
def inserir_dados(df, tabela):

    conn = (conectar_banco())
    cursor = conn.cursor()

    for _, row in df.iterrows():
        # Formatando os valores de acordo com data_types
        dicionario_formatado = {
            coluna: formatar_valor(row[coluna], data_types.get(coluna, 'string'))
            for coluna in df.columns
        }

        # Gerar os campos e valores dinamicamente
        colunas = ', '.join(dicionario_formatado.keys())  # Nomes das colunas
        valores = ', '.join(['%s'] * len(dicionario_formatado))  # Placeholders para os valores

        sql = f"INSERT INTO {tabela} ({colunas}) VALUES ({valores})"
        cursor.execute(sql, tuple(dicionario_formatado.values()))

    # Confirmar as alterações e fechar conexão
    conn.commit()
    cursor.close()
    conn.close()

# Carregar o arquivo Excel e inserir os dados
if __name__ == "__main__":
    try:
        # Carregar o Excel usando pandas
        df = pd.read_excel(excel_file)

        # Inserir os dados no banco
        inserir_dados(df, "bd_tomografia")
        print("Dados inseridos com sucesso!")

    except Exception as e:
        print(f"Erro: {e}")

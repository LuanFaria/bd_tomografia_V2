import pandas as pd
from datetime import datetime
import numpy as np
from db_connection import DatabaseManager

# Define os tipos de dados para reinserção no banco
data_types = {
    'client_id': 'integer', 'client_name': 'string', 'CHAVE': 'string',
    'SAFRA': 'integer', 'OBJETIVO': 'string', 'DT_CORTE': 'string',
    'DT_ULT_CORTE': 'string', 'DT_PLANTIO': 'string',
}

# Função para formatar valores com base no tipo de dado
def formatar_valor(valor, tipo):
    if pd.isnull(valor):  # Trata NaN e NaT como None
        return None
    try:
        if tipo == 'integer':
            return int(valor)
        elif tipo == 'float':
            return float(valor)
        elif tipo == 'date':
            if isinstance(valor, str):  # Converte string no formato Excel para timestamp
                try:
                    dt = datetime.strptime(valor, "%d/%m/%Y %H:%M:%S")
                    return dt.strftime('%Y-%m-%d %H:%M:%S')  # Retorna no formato PostgreSQL
                except ValueError:
                    print(f"Formato de data inválido: {valor}")
                    return None
            elif isinstance(valor, pd.Timestamp):  # Se já for timestamp, apenas retorna
                return valor.strftime('%Y-%m-%d %H:%M:%S')
            return None
        elif tipo == 'string':
            return str(valor)
    except Exception as e:
        print(f"Erro ao formatar o valor '{valor}' do tipo '{tipo}': {e}")
        return None

# Função para obter as colunas da tabela do banco
def obter_colunas_banco(tabela, cursor):
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tabela}'")
    colunas_banco = [col[0].lower() for col in cursor.fetchall()]  # Converte para minúsculo
    return colunas_banco

# Função para inserir os dados no banco
def inserir_dados(df, tabela, db_manager):
    conn = db_manager.get_connection()
    cursor = conn.cursor()

    # Obter as colunas do banco de dados
    colunas_banco = obter_colunas_banco(tabela, cursor)

    for _, row in df.iterrows():
        # Filtra as colunas para inserir apenas as que existem no banco de dados
        dicionario_formatado = {
            coluna.lower(): formatar_valor(row[coluna], data_types.get(coluna, 'string'))
            for coluna in df.columns if coluna.lower() in colunas_banco  # Também converte para minúsculo
        }

        if dicionario_formatado:  # Se houver dados para inserir
            colunas = ', '.join(dicionario_formatado.keys())
            valores = ', '.join(['%s'] * len(dicionario_formatado))

            sql = f"INSERT INTO {tabela} ({colunas}) VALUES ({valores})"
            try:
                cursor.execute(sql, tuple(dicionario_formatado.values()))
            except Exception as e:
                print(f"Erro ao inserir dados: {e}")
                print("SQL:", sql)
                print("Valores:", tuple(dicionario_formatado.values()))

    conn.commit()
    cursor.close()
    conn.close()
    print("Dados inseridos - OK!")

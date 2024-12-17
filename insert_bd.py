import pandas as pd
from datetime import datetime
import numpy as np
from db_connection import DatabaseManager

# Define os tipos de dados para reinserção no banco
data_types = {
    'client_id': 'integer', 'client_name': 'string', 'CHAVE': 'string',
    'SAFRA': 'integer', 'OBJETIVO': 'string', 'DT_CORTE': 'date',
    'DT_ULT_CORTE': 'date', 'DT_PLANTIO': 'date',
}

# Função para formatar valores com base no tipo de dado
def formatar_valor(valor, tipo):
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):  
        return None
    try:
        if tipo == 'integer':
            return int(valor)
        elif tipo == 'float':
            return float(valor)
        elif tipo == 'date':
            if isinstance(valor, int) or (isinstance(valor, str) and valor.isdigit()):
                timestamp = int(valor)
                if timestamp < 0:
                    return "1900-01-01"
                else:
                    return datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
            else:
                return None
        elif tipo == 'string':
            return str(valor)
    except Exception as e:
        print(f"Erro ao formatar o valor '{valor}' do tipo '{tipo}': {e}")
        return None

# Função para inserir os dados no banco usando a conexão compartilhada
def inserir_dados(df, tabela, db_manager):
    conn = db_manager.get_connection()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        dicionario_formatado = {
            coluna: formatar_valor(row[coluna], data_types.get(coluna, 'string'))
            for coluna in df.columns
        }
        colunas = ', '.join(dicionario_formatado.keys())
        valores = ', '.join(['%s'] * len(dicionario_formatado))

        sql = f"INSERT INTO {tabela} ({colunas}) VALUES ({valores})"
        cursor.execute(sql, tuple(dicionario_formatado.values()))

    conn.commit()
    cursor.close()

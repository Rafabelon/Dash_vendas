import os
import pandas as pd
from sqlalchemy import create_engine

# Caminhos
DATA_FOLDER = '/workspaces/Dash_vendas/data'
DB_PATH = '/workspaces/Dash_vendas/database/vendas.db'

# Configurar a conexão com o banco de dados SQLite
engine = create_engine(f'sqlite:///{DB_PATH}')

# Listar todos os arquivos CSV na pasta de dados
csv_files = [f for f in os.listdir(DATA_FOLDER) if f.endswith('.csv')]

# Ler e concatenar todos os arquivos CSV
dataframes = []
for file in csv_files:
    file_path = os.path.join(DATA_FOLDER, file)
    print(f"Lendo o arquivo: {file_path}")
    
    # Tentar ler o arquivo CSV com o separador correto
    try:
        df = pd.read_csv(file_path, sep=';', encoding='utf-8')
    except Exception as e:
        print(f"Erro ao ler com sep=';' e encoding='utf-8': {e}")
        # Tentar com encoding diferente
        df = pd.read_csv(file_path, sep=';', encoding='latin1')
    
    # Remover espaços em branco dos nomes das colunas
    df.columns = [col.strip() for col in df.columns]
    
    dataframes.append(df)

# Concatenar todos os DataFrames
if dataframes:
    data = pd.concat(dataframes, ignore_index=True)
else:
    data = pd.DataFrame()

# Verificar se o DataFrame não está vazio
if not data.empty:
    # Imprimir as colunas disponíveis
    print("Colunas disponíveis no DataFrame após ajuste:")
    print(data.columns.tolist())

    # Lista de colunas monetárias
    col_monetarias = ['VALOR_BRUTO_TRANSACIONADO', 'VALOR_DE_REPASSE']
    
    # Limpar e preparar os dados
    for col in col_monetarias:
        if col in data.columns:
            data[col] = data[col].replace({r'R\$ ': '', r'\.': '', ',': '.'}, regex=True).astype(float)
        else:
            print(f"A coluna '{col}' não foi encontrada no DataFrame.")

    # Converter colunas de data para datetime
    col_datas = ['DATA_DA_TRANSACAO', 'DATA_DO_REPASSE', 'DATA_DA_ANTECIPACAO']
    for col in col_datas:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], errors='coerce')
        else:
            print(f"A coluna '{col}' não foi encontrada no DataFrame.")

    # Persistir os dados no banco de dados SQLite
    data.to_sql('vendas', engine, if_exists='replace', index=False)
    print("Dados inseridos com sucesso no banco de dados.")
else:
    print("O DataFrame está vazio. Verifique se os arquivos CSV estão corretos.")
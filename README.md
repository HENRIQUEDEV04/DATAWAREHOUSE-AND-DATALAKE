# DATAWAREHOUSE-AND-DATALAKE

import pandas as pd
import numpy as np

#
num_produtos = 600
produtos = {
    'produto_id':range(1, num_produtos + 1),
    'nome': [f'Produto {i}' for i in range(1, num_produtos + 1)],
    'categoria': np.random.choice(['Eletrônicos', 'Roupas', 'Alimentos'], num_produtos)

}

#
df_produtos = pd.DataFrame(produtos)

#
num_vendas = 1000 #
data_vendas = {
    'data': np.random.choice(pd.date_range('2024-04-01', periods=30), num_vendas),
    'produto_id': np.random.randint(1, num_produtos + 1, num_vendas),
    'quantidade': np.random.randint(50, 200, num_vendas), #
    'valor_total': np.random.randint(1000,10000, num_vendas) #
}

#
df_vendas = pd.DataFrame(data_vendas)

#
df_vendas.to_csv('vendas.csv', index=False)
df_produtos.to_csv('produtos.csv', index=False)

#
df_vendas = pd.read_csv('vendas.csv')
df_produtos = pd.read_csv('produtos.csv')

#
df_merge = pd.merge(df_vendas, df_produtos, on='produto_id', how='inner')

#
df_merge.to_csv('data_warehouse.csv', index=False)

#
df_warehouse = pd.read_csv('data_warehouse.csv')
print("Conteúdo do Data Warehouse:")
print(df_warehouse)

----


import pandas as pd
import matplotlib.pyplot as plt

#
df_warehouse = pd.read_csv('data_warehouse.csv')

#
vendas_por_produto = df_warehouse.groupby('nome')[['quantidade', 'valor_total']].sum()
print("Análise de vendas por produto:")
print(vendas_por_produto)

#
vendas_por_categoria = df_warehouse.groupby('categoria') [['quantidade', 'valor_total']].sum()
print("\nAnálise de vendas por categoria de produto:")
print(vendas_por_categoria)

#
df_warehouse['data'] = pd.to_datetime(df_warehouse ['data'])
vendas_por_data = df_warehouse.groupby('data')[['quantidade', 'valor_total']].sum()
print("\nAnálise de tendências temporais:")
print(vendas_por_data)

#
plt.figure(figsize=(10, 5))
plt.plot(vendas_por_data.index, vendas_por_data['quantidade'], marker='o', linestyle='-')
plt.title('vendas ao longo do tempo')
plt.xlabel('Data')
plt.ylabel('Quantidade Vendida')
plt.grid(True)
plt.show()

#
desempenho_produto = df_warehouse.groupby('nome')['valor_total'].sum()
print("\nAnálise de desempenho de produtos:")
print(desempenho_produto)

---

import pandas as pd
import numpy as np
import os


#
if not os.path.exists('data_lake'):
  os.makedirs('data_lake')

#
num_files = 10
num_rows_per_file = 1000

#
dfs = []

#
for i in range(num_files):
  #
  data = {
      'coluna1': np.random.randint(0,100, num_rows_per_file),
      'coluna2': np.random.randn(num_rows_per_file),
      'coluna3': np.random.choice(['A', 'B', 'C'], num_rows_per_file)
  }

#
df = pd.DataFrame(data)

#
file_name = f'data_lake/dados_{i+1}.csv'
df.to_csv(file_name, index=False)

#
dfs.append((file_name, df))

print("Dados do Data Lake gerados com sucesso!")

#
for file_name, df in dfs:
  print(f"\nDados do arquivo: {file_name}\n")
  print(df.head())

---

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns 
from sqlalchemy import create_engine 

#
if not os.path.exists('data_lake'):
  os.makedirs('data_lake')

#
num_files = 10
num_rows_per_file = 1000

#
dfs = []

#
for i in range(num_files):
  #
  data = {
      'coluna1': np.random.randint(0, 100, num_rows_per_file),
      'coluna2': np.random.randn(num_rows_per_file),
      'coluna3': np.random.choice(['A', 'B', 'C'], num_rows_per_life)
  }

#
df = pd.DataFrame(data)

#
file_name = f'data_lake/dados_{i+1}.csv'
df.to_csv(file_name, index=False)

#
dfs.append(df)

print("Dados do Data Lake gerados com sucesso!")

#
conn_string = 'sqlite:///data_lake.db'

#
engine = create_engine(conn_string)

#
for i, df in enumerate(dfs, 1):
  table_name = f'dados_{i}'
  df.to_sql(table_name, engine, index=False)




# Criar uma conexão com o banco de dados SQLite
conn_string = 'sqlite:///data_lake.db'

#
engine = create_engine(conn_string)

# Carregar dados do banco de dados para um DataFrame
table_name = 'dados_1'
df = pd.read_sql_table(table_name, engine)

# Imprimir as primeiras linhas do DataFrame
print("Primeiras linhas do DataFrame:")
print(df.head())

# Imprimir informações sobre o DataFrame, incluindo tipo de dados e memória usada
print("\nInformações sobre o DataFrame:")
print(df.info())

# Imprimir um resumo estatístico do DataFrame, incluindo média, desvio padrão, mínimo, máximo, etc.
print("\nResumo estatístico do DataFrame:")
print(df.describe())

# Criar um gráfico de dispersão usando as colunas 'coluna1' e 'coluna2' do DataFrame
plt.figure(figsize=(8,6))
sns.scatterplot(x='coluna1', y='coluna2', data=df)
plt.title("Gráfico de Dispersão entre coluna1 e coluna2")
plt.xlabel('coluna1')
plt.ylabel('coluna2')
plt.grid(True)
plt.show()

# Criar um histograma da coluna 'coluna1' com 20 bins e KDE (Kernel Density Estimation)
plt.figure(figsize=(8,6))
sns.histplot(df['coluna1'], bins=20, kde=True)
plt.title('Histograma da coluna1')
plt.xlabel('coluna1')
plt.ylabel('Frequência')
plt.grid(True)
plt.show()

# Criar um boxplot da coluna 'coluna3' em relação à coluna 'coluna1'
plt.figure(figsize=(8,6))
sns.boxplot(x='coluna3', y='coluna1', data=df)
plt.title('Boxplot da coluna3 em relação a coluna1')
plt.xlabel('coluna3')
plt.ylabel('coluna1')
plt.grid(True)
plt.show()


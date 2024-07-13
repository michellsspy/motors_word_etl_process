import pandas as pd

controler = pd.read_csv('data/controler.csv')
table = 'cidades'

# Filtrando as linhas onde a coluna 'table' é igual a 'table' e selecionando a coluna 'target_bucket'
comand_sql = controler.loc[controler['table'] == table, 'comand_sql'].iloc[0]
target_bucket = controler.loc[controler['table'] == table, 'target_bucket'].iloc[0]
target_folder_path = controler.loc[controler['table'] == table, 'target_folder_path'].iloc[0]   
table = controler.loc[controler['table'] == table, 'table'].iloc[0] 

print(comand_sql)
print(target_folder_path)
print(target_bucket)
print(table)

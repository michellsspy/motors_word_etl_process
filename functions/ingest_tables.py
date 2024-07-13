import psycopg2
import apache_beam as beam
import logging
import pandas as pd
from google.cloud import storage
from google.cloud import secretmanager
import os
import json

class IngestTables(beam.DoFn):
    def process(self, element):
        
        # Recuperando os dados da tabela controller
        controler = pd.read_csv('../data/controler.csv')

        # Criando uma lista vazia para recuperar os nomes das tabelas
        list_tables_names = []
        
        # Recuperando os nomes das tabelas
        list_tables_names.append(table)
        
        # Recuperando os parammetros de conexão com o banco de dados Postgrees 
        # Criando um client
        client = secretmanager.SecretManagerServiceClient()

        # Montando o nome da secret com os dados da conexão
        db_name = "projects/872130982957/secrets/credentials_db/versions/1"
        
        # Acessar o valor da secret
        response = client.access_secret_version(name=db_name)

        # Decodificando o payload
        secret_payload = response.payload.data.decode("UTF-8")
        
        # O payload é um Json - Crinado um dict
        secret_dict = json.loads(secret_payload)
        
        # Definindo as variáveis de ambiente para conexão com o banco    
        os.environ["DB_HOST"] = secret_dict["DB_HOST"] 
        os.environ["DB_PORT"] = secret_dict["DB_PORT"]
        os.environ["DB_NAME"] = secret_dict["DB_NAME"]
        os.environ["DB_USER"] = secret_dict["DB_USER"]
        os.environ["DB_PASSWORD"] = secret_dict["DB_PASSWORD"]
        
        # Recupera as variáveis via variável de ambiente
        USERNAME = os.getenv("DB_USER")
        PASSWORD = os.getenv("DB_PASSWORD")
        HOST = os.getenv("DB_HOST")
        PORT = os.getenv("DB_PORT")
        DATABASE = os.getenv("DB_NAME")
        
        # Criando uma conexão com o banco de dados 
        
        conn_params = {
            "user": USERNAME,
            "password": PASSWORD,
            "host": HOST,
            "port": PORT,
            "database": DATABASE,
        }
        
        for table in element:
            # recuperando valores da tabela controller
            comand_sql = controler.loc[controler['table'] == table, 'comand_sql'].iloc[0]
            where_sql = controler.loc[controler['table'] == table, 'where_sql'].iloc[0]
            limit_sql = controler.loc[controler['table'] == table, 'limit_sql'].iloc[0]
            target_bucket = controler.loc[controler['table'] == table, 'target_bucket'].iloc[0]
            target_folder_path = controler.loc[controler['table'] == table, 'target_folder_path'].iloc[0]             
            
            # Criando uma conexão com o banco de dados 
            try:
                conn = psycopg2.connect(**conn_params)      
                
                # Criando o cursor
                cursor = conn.cursor()
                
                # Montando a query de consulta da tabela
                query = f"{comand_sql} {table} {where_sql}"
                
                cursor.execute(query)
                
                # Recuperando as colunas da tabela
                col_names = [desc[0] for desc in cursor.description]
                
                # Recuperando os dados da consulta
                rows = cursor.fetchall()
                
                # Criando um dataframe
                df_new = pd.DataFrame(rows, columns=col_names)

                logging.info(f'{table} {"=" * (80 - len(table))} {df_new.shape}')
                
                cursor.close()
                conn.close() 
                
                df_new = df_new.astype(str)
                
                # Criando as variáveis do storage client
                bucket_name = target_bucket
                path = target_folder_path
                
                client = storage.Client()
                bucket = client.get_bucket(bucket_name)
                blob = bucket.blob(path)
                
                # Verificando a existência do arquivo no path informado
                if blob.exists():
                    # Lê o arquivo existente no GCS e cria um DataFrame
                    gcs_file_path = f"gs://{bucket_name}/{path}"
                    df_gcs = pd.read_parquet(gcs_file_path)
                    
                    df_gcs = df_gcs.astype(str)
                    
                    # Recupera o nome da primeira coluna para alinhar o merge
                    merge_column = df_gcs.columns[0]
                    
                    # Realiza o merge
                    df_combined = pd.merge(df_gcs, df_new, on=merge_column, how='outer', indicator=True, suffixes=('_old', ''))
                    
                    # Preenche os valores NaN nos dados combinados
                    for column in df_new.columns:
                        old_column = f'{column}_old'
                        if old_column in df_combined.columns:
                            df_combined[column] = df_combined[column].fillna(df_combined[old_column])
                            df_combined.drop(columns=[old_column], inplace=True)
                            
                else:
                    # Cria um novo DataFrame
                    df_combined = df_new
                    
                # Salva o DataFrame combinado (Converte para parquet)
                blob.upload_from_string(df_combined.to_parquet(), content_type='application/x-parquet')
                
                logging.info(f'{table} {"=" * (80 - len(table))} {df_combined.shape}')                                    
                    
            except psycopg2.Error as e:
                logging.info(f"Erro encontrado durante a conexão: {e}")
                yield list_tables_names
            
        yield list_tables_names
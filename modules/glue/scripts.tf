# Glue job scripts as local values
locals {
  raw_job_script = <<-EOF
import sys
import pandas as pd
import boto3
import pymysql
from awsglue.utils import getResolvedOptions
import json

# Get job parameters
args = getResolvedOptions(sys.argv, ['S3_BUCKET'])
s3_bucket = args['S3_BUCKET']

# Database connection parameters
db_host = "REPLACE_WITH_RDS_ENDPOINT"
db_port = 3306
db_name = "etl_database"
db_user = "admin"
db_password = "REPLACE_WITH_PASSWORD"

def process_csv_files():
    s3 = boto3.client('s3')
    
    # List all files in raw-data
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix='raw-data/')
    
    connection = pymysql.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    
    try:
        cursor = connection.cursor()
        
        # Create raw tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_reclamacoes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data_file VARCHAR(255),
                raw_data JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_bancos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data_file VARCHAR(255),
                raw_data JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_empregados (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data_file VARCHAR(255),
                raw_data JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Process files
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv') or key.endswith('.tsv'):
                print(f"Processing {key}")
                
                # Download file
                local_file = f'/tmp/{key.split("/")[-1]}'
                s3.download_file(s3_bucket, key, local_file)
                
                # Read file based on extension
                try:
                    if key.endswith('.tsv'):
                        df = pd.read_csv(local_file, sep='\t', encoding='utf-8')
                    else:
                        df = pd.read_csv(local_file, encoding='utf-8')
                    
                    # Convert to JSON and insert
                    for _, row in df.iterrows():
                        json_data = row.to_json()
                        
                        if 'Reclamacoes' in key:
                            cursor.execute(
                                "INSERT INTO raw_reclamacoes (data_file, raw_data) VALUES (%s, %s)",
                                (key, json_data)
                            )
                        elif 'Bancos' in key:
                            cursor.execute(
                                "INSERT INTO raw_bancos (data_file, raw_data) VALUES (%s, %s)",
                                (key, json_data)
                            )
                        elif 'Empregados' in key:
                            cursor.execute(
                                "INSERT INTO raw_empregados (data_file, raw_data) VALUES (%s, %s)",
                                (key, json_data)
                            )
                except Exception as e:
                    print(f"Error processing {key}: {str(e)}")
                    continue
        
        connection.commit()
        print("Raw layer processing completed successfully")
        
    finally:
        connection.close()

if __name__ == "__main__":
    process_csv_files()
EOF

  trusted_job_script = <<-EOF
import sys
import pandas as pd
import boto3
import pymysql
import pyarrow as pa
import pyarrow.parquet as pq
from awsglue.utils import getResolvedOptions
import json

# Get job parameters
args = getResolvedOptions(sys.argv, ['S3_BUCKET'])
s3_bucket = args['S3_BUCKET']

# Database connection parameters
db_host = "REPLACE_WITH_RDS_ENDPOINT"
db_port = 3306
db_name = "etl_database"
db_user = "admin"
db_password = "REPLACE_WITH_PASSWORD"

def process_trusted_layer():
    connection = pymysql.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    
    s3 = boto3.client('s3')
    
    try:
        cursor = connection.cursor()
        
        # Process reclamacoes
        cursor.execute("SELECT raw_data FROM raw_reclamacoes")
        reclamacoes_data = []
        for row in cursor.fetchall():
            data = json.loads(row[0])
            reclamacoes_data.append(data)
        
        if reclamacoes_data:
            df_reclamacoes = pd.DataFrame(reclamacoes_data)
            # Clean and standardize data
            df_reclamacoes = df_reclamacoes.dropna()
            
            # Save as Parquet to S3
            table = pa.Table.from_pandas(df_reclamacoes)
            pq.write_table(table, '/tmp/trusted_reclamacoes.parquet')
            s3.upload_file('/tmp/trusted_reclamacoes.parquet', s3_bucket, 'trusted-data/reclamacoes.parquet')
        
        # Process bancos
        cursor.execute("SELECT raw_data FROM raw_bancos")
        bancos_data = []
        for row in cursor.fetchall():
            data = json.loads(row[0])
            bancos_data.append(data)
        
        if bancos_data:
            df_bancos = pd.DataFrame(bancos_data)
            df_bancos = df_bancos.dropna()
            
            table = pa.Table.from_pandas(df_bancos)
            pq.write_table(table, '/tmp/trusted_bancos.parquet')
            s3.upload_file('/tmp/trusted_bancos.parquet', s3_bucket, 'trusted-data/bancos.parquet')
        
        # Process empregados
        cursor.execute("SELECT raw_data FROM raw_empregados")
        empregados_data = []
        for row in cursor.fetchall():
            data = json.loads(row[0])
            empregados_data.append(data)
        
        if empregados_data:
            df_empregados = pd.DataFrame(empregados_data)
            df_empregados = df_empregados.dropna()
            
            table = pa.Table.from_pandas(df_empregados)
            pq.write_table(table, '/tmp/trusted_empregados.parquet')
            s3.upload_file('/tmp/trusted_empregados.parquet', s3_bucket, 'trusted-data/empregados.parquet')
        
        print("Trusted layer processing completed successfully")
        
    finally:
        connection.close()

if __name__ == "__main__":
    process_trusted_layer()
EOF

  delivery_job_script = <<-EOF
import sys
import pandas as pd
import boto3
import pymysql
import pyarrow.parquet as pq
from awsglue.utils import getResolvedOptions

# Get job parameters
args = getResolvedOptions(sys.argv, ['S3_BUCKET'])
s3_bucket = args['S3_BUCKET']

# Database connection parameters
db_host = "REPLACE_WITH_RDS_ENDPOINT"
db_port = 3306
db_name = "etl_database"
db_user = "admin"
db_password = "REPLACE_WITH_PASSWORD"

def process_delivery_layer():
    s3 = boto3.client('s3')
    
    # Download trusted parquet files
    try:
        s3.download_file(s3_bucket, 'trusted-data/reclamacoes.parquet', '/tmp/reclamacoes.parquet')
        df_reclamacoes = pd.read_parquet('/tmp/reclamacoes.parquet')
    except:
        df_reclamacoes = pd.DataFrame()
    
    try:
        s3.download_file(s3_bucket, 'trusted-data/bancos.parquet', '/tmp/bancos.parquet')
        df_bancos = pd.read_parquet('/tmp/bancos.parquet')
    except:
        df_bancos = pd.DataFrame()
    
    try:
        s3.download_file(s3_bucket, 'trusted-data/empregados.parquet', '/tmp/empregados.parquet')
        df_empregados = pd.read_parquet('/tmp/empregados.parquet')
    except:
        df_empregados = pd.DataFrame()
    
    # Join and transform data using Python/pandas
    final_df = pd.DataFrame()
    
    if not df_reclamacoes.empty and not df_bancos.empty:
        # Perform joins and transformations
        # This is a simplified example - adjust based on actual data structure
        if 'banco' in df_reclamacoes.columns and 'nome' in df_bancos.columns:
            final_df = pd.merge(df_reclamacoes, df_bancos, left_on='banco', right_on='nome', how='left')
        else:
            final_df = df_reclamacoes
    
    if not final_df.empty and not df_empregados.empty:
        # Add employee data if possible
        if 'empresa' in final_df.columns and 'company' in df_empregados.columns:
            final_df = pd.merge(final_df, df_empregados, left_on='empresa', right_on='company', how='left')
    
    # If no joins possible, create a combined dataset
    if final_df.empty:
        final_df = pd.concat([df_reclamacoes, df_bancos, df_empregados], ignore_index=True, sort=False)
    
    # Save to S3 as Parquet
    if not final_df.empty:
        final_df.to_parquet('/tmp/delivery_final.parquet', index=False)
        s3.upload_file('/tmp/delivery_final.parquet', s3_bucket, 'delivery-data/final_dataset.parquet')
        
        # Save to MySQL database
        connection = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )
        
        try:
            cursor = connection.cursor()
            
            # Create final table
            cursor.execute("DROP TABLE IF EXISTS final_dataset")
            
            # Create table based on DataFrame columns
            columns_sql = []
            for col in final_df.columns:
                columns_sql.append(f"`{col}` TEXT")
            
            create_table_sql = f"""
                CREATE TABLE final_dataset (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    {', '.join(columns_sql)},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            cursor.execute(create_table_sql)
            
            # Insert data
            for _, row in final_df.iterrows():
                placeholders = ', '.join(['%s'] * len(row))
                columns = ', '.join([f"`{col}`" for col in final_df.columns])
                sql = f"INSERT INTO final_dataset ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(row.astype(str)))
            
            connection.commit()
            print(f"Delivery layer completed: {len(final_df)} records processed")
            
        finally:
            connection.close()
    
    else:
        print("No data to process in delivery layer")

if __name__ == "__main__":
    process_delivery_layer()
EOF
}

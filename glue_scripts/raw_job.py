#!/usr/bin/env python3
# Glue Spark Job: S3 (origem) -> S3 (destino) + Glue DB raw (via Spark SQL)

import sys
import re
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import types as T

# =====================================
# Parâmetros do Job
# =====================================
# Exemplo:
# --JOB_NAME job_raw_ingest --SRC_BUCKET src-bucket --DST_BUCKET dst-bucket --GLUE_DB dl_raw
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SRC_BUCKET', 'DST_BUCKET', 'GLUE_DB'])
JOB_NAME   = args['JOB_NAME']
SRC_BUCKET = args['SRC_BUCKET']      # bucket de ORIGEM (CSV/TSV)
DST_BUCKET = args['DST_BUCKET']      # bucket de DESTINO (Parquet + Glue DB LOCATION)
GLUE_DB    = args['GLUE_DB']         # p.ex.: dl_raw (já criado via Terraform)

# Prefixos (podem ser ajustados por convenção)
IN_PREFIX  = "raw"                    # onde estão os CSV/TSV no SRC_BUCKET
OUT_PREFIX = "raw"                    # base do DB raw no DST_BUCKET

# =====================================
# Bootstrap Glue/Spark
# =====================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, {})

# =====================================
# Helpers
# =====================================
def sanitize_column(name: str, existing=None) -> str:
    col = re.sub(r'[^a-zA-Z0-9_]', '_', name)[:60]
    if existing is not None:
        base = col
        i = 1
        while col in existing:
            col = f"{base[:57]}_{i}"
            i += 1
        existing.add(col)
    return col

def rename_columns_safely(df):
    existing = set()
    mapping = {}
    for c in df.columns:
        new_c = sanitize_column(c, existing)
        mapping[c] = new_c
    for old, new in mapping.items():
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df

def read_csv_multi(src_bucket, patterns, sep, encoding):
    paths = [f"s3://{src_bucket}/{p}" for p in patterns]
    try:
        df = (
            spark.read
            .option("header", True)
            .option("sep", sep)
            .option("encoding", encoding)
            .option("inferSchema", True)
            .csv(paths)
        )
        if len(df.columns) == 0:
            return None
        return df
    except Exception as e:
        print(f"Info: nenhum arquivo para {paths} ({e})")
        return None

def write_parquet_and_register_table(df, table_name, dst_bucket, db=GLUE_DB):
    df = rename_columns_safely(df)
    # remover colunas "unnamed"
    for c in df.columns:
        if c.lower().startswith("unnamed"):
            df = df.drop(c)
    # tipos simples/robustos no catálogo
    for c in df.columns:
        df = df.withColumn(c, F.col(c).cast(T.StringType()))

    s3_path = f"s3://{dst_bucket}/{OUT_PREFIX}/{table_name}"

    (
        df.write
          .mode("overwrite")
          .parquet(s3_path)
    )
    print(f"✅ {table_name}: gravado em {s3_path}")

    # Catálogo via Spark SQL
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}` LOCATION 's3://{dst_bucket}/{OUT_PREFIX}/'")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{db}`.`{table_name}`
        USING PARQUET
        LOCATION '{s3_path}'
    """)
    print(f"🔎 Tabela disponível: {db}.{table_name}")

# =====================================
# Pipeline
# =====================================
def main():
    # ------- BANCOS (tab, utf-8) -------
    bancos = read_csv_multi(
        SRC_BUCKET,
        patterns=[f"{IN_PREFIX}/*Bancos*.csv", f"{IN_PREFIX}/*Bancos*.tsv"],
        sep="\t",
        encoding="utf-8"
    )
    if bancos is not None:
        write_parquet_and_register_table(bancos, "raw_bancos", DST_BUCKET)
    else:
        print("ℹ️ Nenhum arquivo Bancos encontrado.")

    # ------- EMPREGADOS (pipe, utf-8) -------
    empregados = read_csv_multi(
        SRC_BUCKET,
        patterns=[f"{IN_PREFIX}/*Empregados*.csv", f"{IN_PREFIX}/*Empregados*.tsv"],
        sep="|",
        encoding="utf-8"
    )
    if empregados is not None:
        write_parquet_and_register_table(empregados, "raw_empregados", DST_BUCKET)
    else:
        print("ℹ️ Nenhum arquivo Empregados encontrado.")

    # ------- RECLAMAÇÕES (ponto-e-vírgula, ISO-8859-1) -------
    reclamacoes = read_csv_multi(
        SRC_BUCKET,
        patterns=[
            f"{IN_PREFIX}/*Reclamacoes*.csv", f"{IN_PREFIX}/*Reclamacoes*.tsv",
            f"{IN_PREFIX}/*tri*.csv",        f"{IN_PREFIX}/*tri*.tsv",
        ],
        sep=";",
        encoding="ISO-8859-1"
    )
    if reclamacoes is not None:
        for c in reclamacoes.columns:
            if c.lower().startswith("unnamed"):
                reclamacoes = reclamacoes.drop(c)
        write_parquet_and_register_table(reclamacoes, "raw_reclamacoes", DST_BUCKET)
    else:
        print("ℹ️ Nenhum arquivo Reclamacoes/tri encontrado.")

    print("🎯 Processamento concluído!")
    job.commit()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# Glue Spark Job: RAW (Glue/S3) -> TRUSTED (Glue/S3) com fallback
# Registra tabelas via Glue Catalog Sink (garante criação/atualização no Data Catalog)

import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import types as T
from awsglue.dynamicframe import DynamicFrame

# =====================================
# Parâmetros do Job
# =====================================
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RAW_DB', 'TRUSTED_DB', 'SRC_BUCKET', 'DST_BUCKET'])
JOB_NAME    = args['JOB_NAME']
RAW_DB      = args['RAW_DB']          # ex.: usp_dl_raw
TRUSTED_DB  = args['TRUSTED_DB']      # ex.: usp_dl_trusted
SRC_BUCKET  = args['SRC_BUCKET']      # bucket da camada RAW
DST_BUCKET  = args['DST_BUCKET']      # bucket da camada TRUSTED

RAW_PREFIX      = "raw"
TRUSTED_PREFIX  = "trusted"

# =====================================
# Bootstrap Glue/Spark
# =====================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, {})

# =====================================
# Helpers de Catalog
# =====================================
def ensure_trusted_db():
    """
    Garante o DB no Glue Catalog apontando para o prefixo TRUSTED no S3.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{TRUSTED_DB}` LOCATION 's3://{DST_BUCKET}/{TRUSTED_PREFIX}/'")

def save_trusted_table(df, table_name):
    """
    Escreve em S3 (Parquet) e REGISTRA/ATUALIZA a tabela no Glue Data Catalog
    usando o Catalog Sink (enableUpdateCatalog=True).
    """
    s3_path = f"s3://{DST_BUCKET}/{TRUSTED_PREFIX}/{table_name}"

    # 1) Converte para DynamicFrame
    dyf = DynamicFrame.fromDF(df, glueContext, f"dyf_{table_name}")

    # 2) Sink configurado para criar/atualizar no Catálogo
    sink = glueContext.getSink(
        connection_type="s3",
        path=s3_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",   # cria se não existir; atualiza schema se existir
        partitionKeys=[],                      # adicione chaves se quiser particionar
        compression="snappy"
    )
    # Formato otimizado do Glue para Parquet
    sink.setFormat("glueparquet")  # ou "parquet" se preferir

    # 3) Info do Catálogo
    sink.setCatalogInfo(catalogDatabase=TRUSTED_DB, catalogTableName=table_name)

    # 4) Grava S3 + registra no Data Catalog
    sink.writeFrame(dyf)

    print(f"✅ {table_name}: TRUSTED em {s3_path} e registrado/atualizado no {TRUSTED_DB}.{table_name}")

# =====================================
# Leitura RAW (igual antes)
# =====================================
def try_read_raw(table_name):
    # 1) via Spark SQL no Catalog
    try:
        df = spark.table(f"`{RAW_DB}`.`{table_name}`")
        if len(df.columns) > 0:
            print(f"🔎 Lido do Catalog via spark.table: {RAW_DB}.{table_name}")
            return df
    except Exception as e:
        print(f"ℹ️ spark.table falhou para {RAW_DB}.{table_name}: {e}")

    # 2) via Glue DynamicFrame (Catalog)
    try:
        dyf = glueContext.create_dynamic_frame.from_catalog(
            database=RAW_DB, table_name=table_name, transformation_ctx=f"read_{table_name}"
        )
        df = dyf.toDF()
        if len(df.columns) > 0:
            print(f"🔎 Lido do Catalog via from_catalog: {RAW_DB}.{table_name}")
            return df
    except Exception as e:
        print(f"ℹ️ from_catalog falhou para {RAW_DB}.{table_name}: {e}")

    # 3) fallback direto do S3 (Parquet gerado no job RAW)
    fallback_path = f"s3://{SRC_BUCKET}/{RAW_PREFIX}/{table_name}"
    try:
        df = spark.read.option("recursiveFileLookup", "true").parquet(fallback_path)
        print(f"🪄 Fallback: lido direto do S3 em {fallback_path}")
        return df
    except Exception as e:
        raise RuntimeError(f"❌ Não foi possível ler {table_name} nem do Catalog nem de {fallback_path}: {e}")

def ci_lookup(df, candidates):
    cols_lc = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lc:
            return cols_lc[cand.lower()]
    return None

def to_double_ptbr(col):
    return F.regexp_replace(F.regexp_replace(F.col(col), r'\.', ''), ',', '.').cast(T.DoubleType())

# =====================================
# Pipeline
# =====================================
def main():
    ensure_trusted_db()

    processed = []

    # -------- trusted_bancos --------
    try:
        rb = try_read_raw("raw_bancos")
        cnpj_col = ci_lookup(rb, ["cnpj"])
        nome_col = ci_lookup(rb, ["nome"])
        seg_col  = ci_lookup(rb, ["segmento"])

        trusted_bancos = rb.select(
            (F.col(cnpj_col) if cnpj_col else F.lit(None)).cast(T.StringType()).alias("cnpj"),
            (F.col(nome_col) if nome_col else F.lit(None)).cast(T.StringType()).alias("nome"),
            (F.col(seg_col)  if seg_col  else F.lit(None)).cast(T.StringType()).alias("segmento"),
        )
        save_trusted_table(trusted_bancos, "trusted_bancos")
        processed.append("trusted_bancos")
    except Exception as e:
        print(f"⚠️ Bancos: {e}")

    # -------- trusted_empregados --------
    try:
        re_df = try_read_raw("raw_empregados")

        mapping_str = {
            "nome": ["nome", "funcionario", "colaborador"],
            "segmento": ["segmento", "area", "departamento"],
            "match_percent": ["match_percent", "match", "aderencia"],
        }
        mapping_num = {
            "geral": ["geral", "score_geral"],
            "cultura_e_valores": ["cultura_e_valores", "cultura_valores"],
            "diversidade_e_inclusao": ["diversidade_e_inclusao", "diversidade_inclusao"],
            "qualidade_de_vida": ["qualidade_de_vida", "qualidade_vida"],
            "alta_lideranca": ["alta_lideranca", "lideranca"],
            "remuneracao_e_beneficios": ["remuneracao_e_beneficios", "remuneracao_beneficios"],
            "oportunidades_de_carreira": ["oportunidades_de_carreira", "oportunidades_carreira"],
        }

        sel_exprs = []
        for out_col, cands in mapping_str.items():
            found = ci_lookup(re_df, cands)
            sel_exprs.append((F.col(found) if found else F.lit(None)).cast(T.StringType()).alias(out_col))
        for out_col, cands in mapping_num.items():
            found = ci_lookup(re_df, cands)
            sel_exprs.append(to_double_ptbr(found).alias(out_col) if found else F.lit(None).cast(T.DoubleType()).alias(out_col))

        trusted_empregados = re_df.select(*sel_exprs)
        save_trusted_table(trusted_empregados, "trusted_empregados")
        processed.append("trusted_empregados")
    except Exception as e:
        print(f"⚠️ Empregados: {e}")

    # -------- trusted_reclamacoes --------
    try:
        rr = try_read_raw("raw_reclamacoes")

        ano_c       = ci_lookup(rr, ["ano"])
        tri_c       = ci_lookup(rr, ["trimestre"])
        cat_c       = ci_lookup(rr, ["categoria"])
        tipo_c      = ci_lookup(rr, ["tipo"])
        cnpj_if_c   = ci_lookup(rr, ["cnpj_if", "cnpj"])
        inst_fin_c  = ci_lookup(rr, ["institui__o_financeira", "instituicao_financeira"])
        indice_c    = ci_lookup(rr, ["_ndice", "indice"])
        tot_recl_c  = ci_lookup(rr, ["quantidade_total_de_reclama__es", "total_reclamacoes"])
        tot_cli_c   = ci_lookup(rr, ["quantidade_total_de_clientes___ccs_e_scr", "total_clientes"])

        trusted_reclamacoes = rr.select(
            (F.col(ano_c).cast(T.IntegerType()) if ano_c else F.lit(None).cast(T.IntegerType())).alias("ano"),
            (F.col(tri_c).cast(T.StringType()) if tri_c else F.lit(None).cast(T.StringType())).alias("trimestre"),
            (F.col(cat_c).cast(T.StringType()) if cat_c else F.lit(None).cast(T.StringType())).alias("categoria"),
            (F.col(tipo_c).cast(T.StringType()) if tipo_c else F.lit(None).cast(T.StringType())).alias("tipo"),
            (F.col(cnpj_if_c).cast(T.StringType()) if cnpj_if_c else F.lit(None).cast(T.StringType())).alias("cnpj_if"),
            (F.col(inst_fin_c).cast(T.StringType()) if inst_fin_c else F.lit(None).cast(T.StringType())).alias("instituicao_financeira"),
            (F.col(indice_c).cast(T.StringType()) if indice_c else F.lit(None).cast(T.StringType()).alias("indice")),
            (F.col(tot_recl_c).cast(T.IntegerType()) if tot_recl_c else F.lit(None).cast(T.IntegerType())).alias("total_reclamacoes"),
            (F.col(tot_cli_c).cast(T.IntegerType()) if tot_cli_c else F.lit(None).cast(T.IntegerType())).alias("total_clientes"),
        )
        save_trusted_table(trusted_reclamacoes, "trusted_reclamacoes")
        processed.append("trusted_reclamacoes")
    except Exception as e:
        print(f"⚠️ Reclamacoes: {e}")

    if processed:
        print(f"🎯 Concluído com sucesso para: {', '.join(processed)}")
    else:
        raise RuntimeError("❌ Nenhuma tabela pôde ser processada. Verifique região/IAM/Catalog.")

    job.commit()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# Glue Spark Job: TRUSTED (Glue/S3) -> DELIVERY (RDS MySQL) [+ cópia S3 opcional]

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

# =========================
# Parâmetros
# =========================
# Exemplo:
# --JOB_NAME job_delivery --TRUSTED_DB usp_dl_trusted --S3_BUCKET usp-datalake-trusted-... (para cópia opcional)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TRUSTED_DB', 'S3_BUCKET'])
JOB_NAME    = args['JOB_NAME']
TRUSTED_DB  = args['TRUSTED_DB']
S3_BUCKET   = args['S3_BUCKET']   # usado para salvar cópia Parquet opcional em s3://{S3_BUCKET}/delivery/delivery_bancos/

TRUSTED_PREFIX  = "trusted"
DELIVERY_PREFIX = "delivery"  # somente para cópia S3 opcional

# =========================
# Config RDS MySQL (ajuste aqui)
# =========================
db_host      = "dev-mysql-db.cydoago2ah4b.us-east-1.rds.amazonaws.com"
db_port      = 3306
delivery_db  = "delivery_data"
db_user      = "admin"
db_password  = "your-secure-password-here"

JDBC_BASE     = f"jdbc:mysql://{db_host}:{db_port}"
JDBC_DELIVERY = f"{JDBC_BASE}/{delivery_db}?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC&allowMultiQueries=true&rewriteBatchedStatements=true"

jdbc_props = {
    "user": db_user,
    "password": db_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# =========================
# Bootstrap
# =========================
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(JOB_NAME, {})

# =========================
# Helpers
# =========================
def execute_sql(url: str, sql: str):
    """
    Executa SQL via JDBC (DriverManager) no lado JVM do Spark.
    Aceita múltiplos statements separados por ';'.
    """
    conn = None
    try:
        DriverManager = sc._jvm.java.sql.DriverManager
        conn = DriverManager.getConnection(url, db_user, db_password)
        stmt = conn.createStatement()
        for s in [x.strip() for x in sql.split(';') if x.strip()]:
            stmt.execute(s)
        stmt.close()
    finally:
        if conn:
            conn.close()

def normalize_name_col(col_name: str):
    """
    UPPER -> remove ' PRUDENCIAL' -> remove não [A-Z0-9 ] -> trim
    """
    c = F.upper(F.col(col_name))
    c = F.regexp_replace(c, r' PRUDENCIAL', '')
    c = F.regexp_replace(c, r'[^A-Z0-9 ]', '')
    return F.trim(c)

def normalize_cnpj_col(col_name: str):
    return F.regexp_replace(F.col(col_name), r'\D', '')

# =========================
# Leitura TRUSTED (Glue Catalog)
# =========================
bancos       = spark.table(f"`{TRUSTED_DB}`.`trusted_bancos`")
empregados   = spark.table(f"`{TRUSTED_DB}`.`trusted_empregados`")
reclamacoes  = spark.table(f"`{TRUSTED_DB}`.`trusted_reclamacoes`")

# =========================
# Normalizações
# =========================
bancos = (bancos
          .withColumn("nome_norm", normalize_name_col("nome"))
          .withColumn("cnpj_norm", normalize_cnpj_col("cnpj")))

empregados = (empregados
              .withColumn("nome_norm", normalize_name_col("nome")))

reclamacoes = (reclamacoes
               .withColumn("nome_norm", normalize_name_col("instituicao_financeira"))
               .withColumn("cnpj_if_norm", normalize_cnpj_col("cnpj_if")))

# =========================
# JOIN 1: bancos + empregados por nome_norm (left)
# =========================
emp_sel = empregados.select(
    "nome_norm",
    F.col("match_percent").cast(T.StringType()).alias("match_percent"),
    F.col("geral").cast(T.DoubleType()).alias("geral"),
    F.col("cultura_e_valores").cast(T.DoubleType()).alias("cultura_e_valores"),
    F.col("diversidade_e_inclusao").cast(T.DoubleType()).alias("diversidade_e_inclusao"),
    F.col("qualidade_de_vida").cast(T.DoubleType()).alias("qualidade_de_vida"),
    F.col("alta_lideranca").cast(T.DoubleType()).alias("alta_lideranca"),
    F.col("remuneracao_e_beneficios").cast(T.DoubleType()).alias("remuneracao_e_beneficios"),
    F.col("oportunidades_de_carreira").cast(T.DoubleType()).alias("oportunidades_de_carreira"),
)

merged = (bancos
          .join(emp_sel, on="nome_norm", how="left"))

# =========================
# JOIN 2: adiciona reclamações por CNPJ
# =========================
rec_sel = reclamacoes.select(
    "cnpj_if_norm",
    F.col("ano").cast(T.IntegerType()).alias("ano"),
    F.col("trimestre").cast(T.StringType()).alias("trimestre"),
    F.col("categoria").cast(T.StringType()).alias("categoria"),
    F.col("tipo").cast(T.StringType()).alias("tipo"),
    F.col("indice").cast(T.StringType()).alias("indice"),
    F.col("total_reclamacoes").cast(T.IntegerType()).alias("total_reclamacoes"),
    F.col("total_clientes").cast(T.IntegerType()).alias("total_clientes"),
)

merged = (merged
          .join(rec_sel, merged.cnpj_norm == rec_sel.cnpj_if_norm, "left"))

# =========================
# Projeção final (ordem dos campos)
# =========================
final_df = merged.select(
    F.col("cnpj").cast(T.StringType()).alias("cnpj"),
    F.col("nome").cast(T.StringType()).alias("nome"),
    F.col("segmento").cast(T.StringType()).alias("segmento"),
    F.col("match_percent"),
    F.col("geral"),
    F.col("cultura_e_valores"),
    F.col("diversidade_e_inclusao"),
    F.col("qualidade_de_vida"),
    F.col("alta_lideranca"),
    F.col("remuneracao_e_beneficios"),
    F.col("oportunidades_de_carreira"),
    F.col("ano"),
    F.col("trimestre"),
    F.col("categoria"),
    F.col("tipo"),
    F.col("indice"),
    F.col("total_reclamacoes"),
    F.col("total_clientes"),
)

# =========================
# Cria DB/Tabela no MySQL (se não existirem)
# =========================
# 1) DB
execute_sql(f"{JDBC_BASE}/?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC&allowMultiQueries=true",
            f"CREATE DATABASE IF NOT EXISTS `{delivery_db}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;")

# 2) Tabela tipada + AUTO_INCREMENT + created_at
ddl_cols = [
    "`cnpj` VARCHAR(20)",
    "`nome` VARCHAR(255)",
    "`segmento` VARCHAR(100)",
    "`match_percent` VARCHAR(32)",
    "`geral` DOUBLE",
    "`cultura_e_valores` DOUBLE",
    "`diversidade_e_inclusao` DOUBLE",
    "`qualidade_de_vida` DOUBLE",
    "`alta_lideranca` DOUBLE",
    "`remuneracao_e_beneficios` DOUBLE",
    "`oportunidades_de_carreira` DOUBLE",
    "`ano` INT",
    "`trimestre` VARCHAR(16)",
    "`categoria` VARCHAR(255)",
    "`tipo` VARCHAR(255)",
    "`indice` VARCHAR(255)",
    "`total_reclamacoes` INT",
    "`total_clientes` INT"
]
pre_sql = f"""
DROP TABLE IF EXISTS `{delivery_db}`.`delivery_bancos`;
CREATE TABLE `{delivery_db}`.`delivery_bancos`(
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  {', '.join(ddl_cols)},
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
execute_sql(JDBC_DELIVERY, pre_sql)

# =========================
# Grava em lote via JDBC (append)
# =========================
(final_df
 .write
 .format("jdbc")
 .option("url", JDBC_DELIVERY)
 .option("dbtable", "delivery_bancos")
 .option("user", db_user)
 .option("password", db_password)
 .option("driver", "com.mysql.cj.jdbc.Driver")
 .option("batchsize", "10000")
 .mode("append")
 .save())

print(f"✅ Delivery (MySQL) populada: delivery_data.delivery_bancos")

# =========================
# (Opcional) Cópia Parquet no S3
# =========================
s3_out = f"s3://{S3_BUCKET}/{DELIVERY_PREFIX}/delivery_bancos"
(final_df.write.mode("overwrite").parquet(s3_out))
print(f"📦 Cópia Parquet salva em {s3_out}")

job.commit()

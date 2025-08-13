# Upload individual files from Dados folder
resource "aws_s3_object" "reclamacoes_2021_tri_01" {
  bucket = module.s3.bucket_name
  key    = "raw/Reclamacoes/2021_tri_01.csv"
  source = "./Dados/Reclamacoes/2021_tri_01.csv"
  etag   = filemd5("./Dados/Reclamacoes/2021_tri_01.csv")
}

resource "aws_s3_object" "reclamacoes_2021_tri_02" {
  bucket = module.s3.bucket_name
  key    = "raw/Reclamacoes/2021_tri_02.csv"
  source = "./Dados/Reclamacoes/2021_tri_02.csv"
  etag   = filemd5("./Dados/Reclamacoes/2021_tri_02.csv")
}

resource "aws_s3_object" "reclamacoes_2021_tri_03" {
  bucket = module.s3.bucket_name
  key    = "raw/Reclamacoes/2021_tri_03.csv"
  source = "./Dados/Reclamacoes/2021_tri_03.csv"
  etag   = filemd5("./Dados/Reclamacoes/2021_tri_03.csv")
}

resource "aws_s3_object" "reclamacoes_2021_tri_04" {
  bucket = module.s3.bucket_name
  key    = "raw/Reclamacoes/2021_tri_04.csv"
  source = "./Dados/Reclamacoes/2021_tri_04.csv"
  etag   = filemd5("./Dados/Reclamacoes/2021_tri_04.csv")
}

resource "aws_s3_object" "reclamacoes_2022_tri_01" {
  bucket = module.s3.bucket_name
  key    = "raw/Reclamacoes/2022_tri_01.csv"
  source = "./Dados/Reclamacoes/2022_tri_01.csv"
  etag   = filemd5("./Dados/Reclamacoes/2022_tri_01.csv")
}

resource "aws_s3_object" "reclamacoes_2022_tri_03" {
  bucket = module.s3.bucket_name
  key    = "raw/Reclamacoes/2022_tri_03.csv"
  source = "./Dados/Reclamacoes/2022_tri_03.csv"
  etag   = filemd5("./Dados/Reclamacoes/2022_tri_03.csv")
}

resource "aws_s3_object" "reclamacoes_2022_tri_04" {
  bucket = module.s3.bucket_name
  key    = "raw/Reclamacoes/2022_tri_04.csv"
  source = "./Dados/Reclamacoes/2022_tri_04.csv"
  etag   = filemd5("./Dados/Reclamacoes/2022_tri_04.csv")
}

resource "aws_s3_object" "bancos_enquadramento" {
  bucket = module.s3.bucket_name
  key    = "raw/Bancos/EnquadramentoInicia_v2.tsv"
  source = "./Dados/Bancos/EnquadramentoInicia_v2.tsv"
  etag   = filemd5("./Dados/Bancos/EnquadramentoInicia_v2.tsv")
}

resource "aws_s3_object" "empregados_glassdoor" {
  bucket = module.s3.bucket_name
  key    = "raw/Empregados/glassdoor_consolidado_join_match_v2.csv"
  source = "./Dados/Empregados/glassdoor_consolidado_join_match_v2.csv"
  etag   = filemd5("./Dados/Empregados/glassdoor_consolidado_join_match_v2.csv")
}

resource "aws_s3_object" "empregados_glassdoor_less" {
  bucket = module.s3.bucket_name
  key    = "raw/Empregados/glassdoor_consolidado_join_match_less_v2.csv"
  source = "./Dados/Empregados/glassdoor_consolidado_join_match_less_v2.csv"
  etag   = filemd5("./Dados/Empregados/glassdoor_consolidado_join_match_less_v2.csv")
}

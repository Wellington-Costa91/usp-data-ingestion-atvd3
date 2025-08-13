# ETL Pipeline Terraform Infrastructure

Esta solução implementa um pipeline ETL completo usando AWS Step Functions, Glue Jobs e RDS MySQL, seguindo as especificações do exercício 2.

## Arquitetura

### Componentes:
- **VPC**: Rede privada com subnets públicas e privadas
- **S3**: Armazenamento de dados (Raw, Trusted, Delivery)
- **RDS MySQL**: Banco de dados relacional em subnets privadas
- **Glue Jobs**: 3 jobs para processamento das camadas (Raw, Trusted, Delivery)
- **Step Functions**: Orquestração do pipeline ETL
- **VPC Endpoints**: Conectividade segura para S3 e Glue

### Camadas de Dados:
1. **RAW**: Dados originais no MySQL
2. **Trusted**: Dados limpos em formato Parquet no S3 e tabela final (MySQL)
3. **Delivery**: Dados finais unidos em Parquet (S3) e tabela final (MySQL)

### Segurança:
- RDS em subnets privadas
- Security Groups restritivos
- Glue Connection para acesso seguro ao banco
- VPC Endpoints para tráfego interno
- Criptografia habilitada no S3 e RDS

## Requisitos Atendidos

✅ **Linguagem Python**: Todos os jobs Glue usam Python puro  
✅ **Sem Spark/DuckDB**: Usa pandas, pyarrow, pymysql  
✅ **Banco Relacional**: MySQL RDS em VPC privada  
✅ **Tratamento via Python**: Não usa SQL para transformações  
✅ **Formatos Parquet**: Trusted e Delivery em Parquet  
✅ **Tabela Final**: Dados unidos na tabela `final_dataset`  
✅ **Infraestrutura Completa**: VPC, Subnets, Security Groups, Connections

## Deploy

1. Copie o arquivo de variáveis:
```bash
cp terraform.tfvars.example terraform.tfvars
```

2. Edite as variáveis necessárias (especialmente bucket name único):
```bash
vim terraform.tfvars
```

3. Inicialize e aplique o Terraform:
```bash
terraform init
terraform plan
terraform apply
```

## Execução do Pipeline

Execute o Step Function via AWS Console ou CLI:
```bash
aws stepfunctions start-execution \
  --state-machine-arn <STATE_MACHINE_ARN> \
  --name "etl-execution-$(date +%s)"
```

## Estrutura de Rede

- **VPC**: 10.0.0.0/16
- **Subnets Públicas**: 10.0.1.0/24, 10.0.2.0/24 (NAT Gateways)
- **Subnets Privadas**: 10.0.10.0/24, 10.0.20.0/24 (RDS, Glue)
- **Internet Gateway**: Acesso público
- **NAT Gateways**: Acesso internet para recursos privados
- **VPC Endpoints**: S3 e Glue para tráfego interno

## Estrutura de Dados

Os dados da pasta `Dados/` são automaticamente carregados no S3 e processados através das 3 camadas:

- **Reclamações**: CSVs trimestrais de 2021-2022
- **Bancos**: TSV com enquadramento inicial  
- **Empregados**: CSVs do Glassdoor

A tabela final `final_dataset` contém todos os dados unidos e tratados.

## Monitoramento

- CloudWatch Logs para Glue Jobs
- Step Functions execution history
- RDS Performance Insights (opcional)
- VPC Flow Logs (se habilitado)

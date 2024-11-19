# watsonx-data

# Executar consultas for watsonx.data

## Configuração das credenciais

Esta célula define as credenciais para acessar o watsonx.data. As credenciais incluem:

wxd_hms_endpoint: o endpoint do serviço Watsonx.data
wxd_hms_username: o nome de usuário para acessar o Watsonx.data
wxd_hms_password: a senha para acessar o Watsonx.data
source_bucket_endpoint: o endpoint do bucket de armazenamento de dados
source_bucket_access_key: a chave de acesso para o bucket de armazenamento de dados
source_bucket_secret_key: a chave secreta para o bucket de armazenamento de dados

```
wxd_hms_endpoint = "thrift://aaaaaaaa-bbbb-cccc-dddd-abcdef012345.abcdefghijk0123456789.lakehouse.appdomain.cloud:12345"
wxd_hms_username = "ibmlhapikey"
wxd_hms_password = "api_key_IAM"
source_bucket_endpoint = "s3.br-sao.cloud-object-storage.appdomain.cloud"
source_bucket_access_key = "your_source_access_key"
source_bucket_secret_key = "e3fc2cfe2d31396fd2e0b110533b92e8fbcc68c7d75c1ae4"
```

## Conexão

Agora vamos realizar a configuração da conexão com o watsonx.data

```
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

# Configuração Spark para wxd
conf = spark.sparkContext.getConf()
spark.stop()

conf.setAll([("spark.sql.catalogImplementation", "hive"), \
    ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"), \
    ("spark.sql.iceberg.vectorization.enabled", "false"), \
    ("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog"), \
    ("spark.sql.catalog.lakehouse.type", "hive"), \
    ("spark.sql.catalog.lakehouse.uri", wxd_hms_endpoint), \
    ("spark.hive.metastore.client.auth.mode", "PLAIN"), \
    ("spark.hive.metastore.client.plain.username", wxd_hms_username), \
    ("spark.hive.metastore.client.plain.password", wxd_hms_password), \
    ("spark.hive.metastore.use.SSL", "true"), \
    ("spark.hive.metastore.truststore.type", "JKS"), \
    ("spark.hive.metastore.truststore.path", "file:///opt/ibm/jdk/lib/security/cacerts"), \
    ("spark.hive.metastore.truststore.password", "changeit"), \
    ("spark.hadoop.fs.s3a.bucket.wxd-silver.endpoint", source_bucket_endpoint), \
    ("spark.hadoop.fs.s3a.bucket.wxd-silver.access.key", source_bucket_access_key), \
    ("spark.hadoop.fs.s3a.bucket.wxd-silver.secret.key", source_bucket_secret_key), \
])

spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

### Listando os databases watsonx.data


Uma vez que temos a conexão pronta, podemos listar os bancos de dados existentes no watsonx.data

```
def list_databases(spark):
    spark.sql("show databases from lakehouse").show()

list_databases(spark)
```

### Executando consulta de um catálogo específico 


```
spark.sql("show tables from lakehouse.teste_silver").show()
```

Você pode executar a consulta via engine Spark e transformar em um DataFrame pandas para futuras análises.

```
df_spark = spark.sql("""
SELECT 
  *
FROM
  lakehouse.teste_silver.cadastro
""").toPandas()
```

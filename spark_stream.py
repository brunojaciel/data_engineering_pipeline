import logging
import csv
import gzip
import uuid
from dags import api_data
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id TEXT PRIMARY KEY,
        codlegislatura TEXT,
        datemissao TEXT,
        idedocumento TEXT,
        idecadastro TEXT,
        indtipodocumento TEXT,
        nucarteiraparlamentar TEXT,
        nudeputadoid TEXT,
        nulegislatura TEXT,
        numano TEXT,
        numespecificacaosubcota TEXT,
        numlote TEXT,
        nummes TEXT,
        numparcela TEXT,
        numressarcimento TEXT,
        numsubcota TEXT,
        sgpartido TEXT,
        sguf TEXT,
        txnomeparlamentar TEXT,
        txtcnpjcpf TEXT,
        txtdescricao TEXT,
        txtdescricaoespecificacao TEXT,
        txtfornecedor TEXT,
        txtnumero TEXT,
        txtpassageiro TEXT,
        txttrecho TEXT,
        vlrdocumento TEXT,
        vlrglosa TEXT,
        vlrliquido TEXT,
        vlrrestituicao TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    #print("inserting data...")

    id = str(uuid.uuid4())
    codlegislatura = kwargs.get('codlegislatura')
    datemissao = kwargs.get('datemissao')
    idedocumento = kwargs.get('idedocumento')
    idecadastro = kwargs.get('idecadastro')
    indtipodocumento = kwargs.get('indtipodocumento')
    nucarteiraparlamentar = kwargs.get('nucarteiraparlamentar')
    nudeputadoid = kwargs.get('nudeputadoid')
    nulegislatura = kwargs.get('nulegislatura')
    numano = kwargs.get('numano')
    numespecificacaosubcota = kwargs.get('numespecificacaosubcota')
    numlote = kwargs.get('numlote')
    nummes = kwargs.get('nummes')
    numparcela = kwargs.get('numparcela')
    numressarcimento = kwargs.get('numressarcimento')
    numsubcota = kwargs.get('numsubcota')
    sgpartido = kwargs.get('sgpartido')
    sguf = kwargs.get('sguf')
    txnomeparlamentar = kwargs.get('txnomeparlamentar')
    txtcnpjcpf = kwargs.get('txtcnpjcpf')
    txtdescricao = kwargs.get('txtdescricao')
    txtdescricaoespecificacao = kwargs.get('txtdescricaoespecificacao')
    txtfornecedor = kwargs.get('txtfornecedor')
    txtnumero = kwargs.get('txtnumero')
    txtpassageiro = kwargs.get('txtpassageiro')
    txttrecho = kwargs.get('txttrecho')
    vlrdocumento = kwargs.get('vlrdocumento')
    vlrglosa = kwargs.get('vlrglosa')
    vlrliquido = kwargs.get('vlrliquido')
    vlrrestituicao = kwargs.get('vlrrestituicao')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, codlegislatura, datemissao, idedocumento, idecadastro, indtipodocumento, 
                nucarteiraparlamentar, nudeputadoid, nulegislatura, numano, numespecificacaosubcota, 
                numlote, nummes, numparcela, numressarcimento, numsubcota, sgpartido, sguf, 
                txnomeparlamentar, txtcnpjcpf, txtdescricao, txtdescricaoespecificacao, 
                txtfornecedor, txtnumero, txtpassageiro, txttrecho, vlrdocumento, vlrglosa, 
                vlrliquido, vlrrestituicao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s)
        """, (id, codlegislatura, datemissao, idedocumento, idecadastro,
              indtipodocumento, nucarteiraparlamentar, nudeputadoid, nulegislatura,
              numano, numespecificacaosubcota, numlote, nummes, numparcela,
              numressarcimento, numsubcota, sgpartido, sguf, txnomeparlamentar,
              txtcnpjcpf, txtdescricao, txtdescricaoespecificacao, txtfornecedor,
              txtnumero, txtpassageiro, txttrecho, vlrdocumento, vlrglosa, vlrliquido,
              vlrrestituicao))
        #logging.info("Data inserted")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'parliamentary_quota_data') \
            .option("includeHeaders", "true") \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("codlegislatura", StringType(), True),
        StructField("datemissao", StringType(), True),
        StructField("idedocumento", StringType(), True),
        StructField("idecadastro", StringType(), True),
        StructField("indtipodocumento", StringType(), True),
        StructField("nucarteiraparlamentar", StringType(), True),
        StructField("nudeputadoid", StringType(), True),
        StructField("nulegislatura", StringType(), True),
        StructField("numano", StringType(), True),
        StructField("numespecificacaosubcota", StringType(), True),
        StructField("numlote", StringType(), True),
        StructField("nummes", StringType(), True),
        StructField("numparcela", StringType(), True),
        StructField("numressarcimento", StringType(), True),
        StructField("numsubcota", StringType(), True),
        StructField("sgpartido", StringType(), True),
        StructField("sguf", StringType(), True),
        StructField("txnomeparlamentar", StringType(), True),
        StructField("txtcnpjcpf", StringType(), True),
        StructField("txtdescricao", StringType(), True),
        StructField("txtdescricaoespecificacao", StringType(), True),
        StructField("txtfornecedor", StringType(), True),
        StructField("txtnumero", StringType(), True),
        StructField("txtpassageiro", StringType(), True),
        StructField("txttrecho", StringType(), True),
        StructField("vlrdocumento", StringType(), True),
        StructField("vlrglosa", StringType(), True),
        StructField("vlrliquido", StringType(), True),
        StructField("vlrrestituicao", StringType(), True)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        #selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            # Caso não tenha, cadastre-se no Brasil.io e gere seu Token
            # Para mais instruções: https://blog.brasil.io/2020/10/10/como-acessar-os-dados-do-brasil-io/
            user_agent = "brunojaciel"
            auth_token = "972fcc73b93d75aa832018e4023ea663ef98c69d"

            api = api_data.BrasilIO(user_agent, auth_token)

            dataset_slug = "gastos-deputados"
            table_name = "cota_parlamentar"

            # Download do arquivo completo
            file_path = f"{dataset_slug}_{table_name}.csv.gz"
            api.download(dataset_slug, table_name, file_path)

            # Read the CSV file and insert data row by row
            with gzip.open(file_path, mode="rt") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    insert_data(session, **row)

            print("Finish")
            logging.info("Data inserted")
            """
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()
            """

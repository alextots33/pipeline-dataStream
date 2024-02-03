import logging
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType


 # fonction qui crée le keyspace dans Cassandra
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH REPLICATION = {
            'class' : 'SimpleStrategy',
            'replication_factor' : 1
        }  
    """)
    print("Keyspace created successfully")


# fonction qui crée la table dans Cassandra
def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS users.users_created (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
    """)
    print("Table created successfully")


# fonction qui insère les données dans Cassandra
def insert_data(session, **kwargs):
    """Insert data into Cassandra table"""
    print("Inserting data...")

    user_id = kwargs.get("id")
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture') 

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                        postcode, email, username, dob, phone, picture)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (user_id, first_name, last_name, gender, address, postcode, email, username, registered_date, phone, picture))
        logging.info("Data inserted successfully")
    except Exception as e:
        logging.error(f"Error while inserting data: {e}")
        return None  


# fonction qui crée une connexion à Spark
def create_spark_connection():
    """Create Spark session and return it""" 
    spark_conn = None
    try:
        spark_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .master("spark://spark:7077") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"\
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .getOrCreate()
        
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully")
        return spark_conn
    except Exception as e:
        logging.error(f"Error while creating Spark session: {e}")
        return None


# fonction qui crée une connexion à Kafka
def connect_to_kafka(spark_conn):
    """Connect to Kafka and return a Spark DataFrame"""
    spark_df = None
    try:
        spark_df = spark_conn \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Connected to Kafka successfully")
        return spark_df
    except Exception as e:
        logging.error(f"Error while connecting to Kafka: {e}")
        return None


# fonction qui crée une connexion à Cassandra
def create_cassandre_connection():
    """Create Cassandra connection and return it"""
    cassandra_session = None
    try:
        cluster = Cluster(['localhost'], port=9042)
        cassandra_session = cluster.connect()
        return cassandra_session
    except Exception as e:
        logging.error(f"Error while creating Cassandra connection: {e}")
        return None


# fonction qui crée un DataFrame à partir de Kafka
def create_selection_df_from_kafka(spark_df):
    """Create a selection DataFrame from Kafka"""
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # On sélectionne les données de la colonne "value" et on les convertit en string
    select = spark_df.selectRxpr("CAST(value AS STRING)")\
        .select(from_json("value", schema).alias("data")).select("data.*")
    return select


if __name__ == "__main__":
    # Connexion à Spark
    spark_conn = create_spark_connection()

    # Si la connexion à Spark est réussie
    if spark_conn is not None:
        # Connexion à Kafka avec Spark
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        session = create_cassandre_connection()

        # Si la connexion à Kafka est réussie
        if session is not None:
            create_keyspace(session)
            create_table(session)
            insert_data(session)

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")\
                .option("checkpointLocation", "/tmp/checkpoint")\
                .option("keyspace", "spark_streams")\
                .option("table", "created_users")\
                .start())
            streaming_query.awaitTermination()
        else:
            logging.error("Error while connecting to Cassandra")


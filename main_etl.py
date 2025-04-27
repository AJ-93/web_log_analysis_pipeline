from pyspark.sql import *
from pyspark.sql.functions import regexp_extract, substring_index
from pyspark.sql.functions import to_timestamp

from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=WebLogAnalysisLogFile")
    conf_2 = SparkConf()
    conf_2.set("spark.jars", "Jars/postgresql-42.7.2.jar")
    conf_1 = load_spark_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .config(conf=conf_1) \
        .config(conf=conf_2) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4J(spark)

    file_df = spark.read.text("/Users/a.chadayan/Documents/Data/access.log")

    logger.info(file_df.printSchema())

    log_regex = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    logs_df = file_df.select(regexp_extract("value", log_regex, 1).alias("ip"),
                regexp_extract("value", log_regex, 4).alias("timestamp"),
                regexp_extract("value", log_regex, 6).alias("request"),
                regexp_extract("value", log_regex, 10).alias("referrer"),
                             )
    logs_df.withColumn("timestamp", to_timestamp("timestamp", "dd/MMM/yyyy HH:mm:ss Z"))
    logs_df.show()

    # Define the JDBC connection properties
    jdbc_url = "jdbc:postgresql://localhost:5432/log_analysis"
    jdbc_properties = {
        "user": "YWpheQ==",
        "password": "cGFzc3dvcmQ=",
        "driver": "org.postgresql.Driver"  # Ensure you have the PostgreSQL JDBC driver
    }

    logs_df.write.jdbc(url=jdbc_url, table= 'apache_web_logs' ,properties=jdbc_properties)

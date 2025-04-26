from pyspark.sql import *
from pyspark.sql.functions import regexp_extract, substring_index

from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=WebLogAnalysisLogFile")
    conf_1 = load_spark_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .config(conf=conf_1) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4J(spark)

    file_df = spark.read.text("Data/access.log")

    logger.info(file_df.printSchema())

    log_regex = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    logs_df = file_df.select(regexp_extract("value", log_regex, 1).alias("ip"),
                regexp_extract("value", log_regex, 4).alias("date"),
                regexp_extract("value", log_regex, 6).alias("request"),
                regexp_extract("value", log_regex, 10).alias("referrer"),
                             )
    logs_df.show()




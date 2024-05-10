from pyspark.sql import SparkSession


class Log4j:
    """
    This class wraps around Log4j JVM objects provided by PySpark to enable logging via Log4j in PySpark.
    """

    def __init__(self, spark: SparkSession, root_class: str = "sk.spark.examples") -> None:
        """
        Initializes a Log4j instance for the given Spark session and root class for logging.
        :param spark: SparkSession object.
        :param root_class: The root package name where the application classes are defined.
        """
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(f"{root_class}.{app_name}")

    def warn(self, message: str) -> None:
        """Log a message at the WARN level."""
        self.logger.warn(message)

    def info(self, message: str) -> None:
        """Log a message at the INFO level."""
        self.logger.info(message)

    def error(self, message: str) -> None:
        """Log a message at the ERROR level."""
        self.logger.error(message)

    def debug(self, message: str) -> None:
        """Log a message at the DEBUG level."""
        self.logger.debug(message)

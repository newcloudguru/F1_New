class Log4j(object):
    # Wrapper class for Log4j JVM
    # param spark: SparkSession

    def __init__(self, spark):
        # retrieve spark app details
        conf = spark.sparkContext.getConf()
        id = conf.get('spark.app.id')
        name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + name + ' ' + id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        # Log an error.

        self.logger.error(message)
        return None

    def warn(self, message):
        # log a warning.

        self.logger.warn(message)
        return None

    def info(self, message):
        # log information.

        self.logger.info(message)
        return None

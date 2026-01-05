


class Log4J(object):

    def __init__(self, spark):
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4J = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4J.LogManager.getLogger(message_prefix)

    def error(self,message):
        self.logger.error(message)
        return None
    def warn(self,message):
        self.logger.warn(message)
        return None
    def info(self,message):
        self.logger.info(message)
        return None

from pyspark.sql import SparkSession


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class SingletonMetaBasic(metaclass=SingletonMeta):
    pass


class SessionInit:
    @staticmethod
    def get_spark_session() -> SparkSession:
        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            # TODO: invoke the spark session initialization implicitly for further version
            raise RuntimeError('Spark session has not been init')

        return spark_session

from pyspark.sql import SparkSession


def main():
    print("Hello from realtime-clickstream-pyspark!")
    spark = SparkSession.builder.appName('VersionCheck').getOrCreate()
    print("Running spark version:", spark.version)
    spark.stop()


if __name__ == "__main__":
    main()

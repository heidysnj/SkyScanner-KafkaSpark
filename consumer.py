import sys
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName = 'skyScannerApp')
spark = SparkSession(sc)
ssc = StreamingContext(sc, 10)
directKafkaStream = KafkaUtils.createDirectStream(ssc, ['skyScanner'], {'metadata.broker.list': 'localhost:9098'})

parsed = directKafkaStream.map(lambda v: json.loads(v[1]))

def makeIterable(rdd):
    for x in rdd.collect():
        df = spark.createDataFrame(x['Places'])
        df.write.format('jdbc').options( 
            url='jdbc:mysql://localhost/SkyScannerKafka',  
            driver='com.mysql.jdbc.Driver',
            dbtable='placeSky',
            user='heidys',
            password='LiamCruz..09302020').mode('overwrite').save()
        df.show()

parsed.foreachRDD(makeIterable)

ssc.start()
ssc.awaitTermination()
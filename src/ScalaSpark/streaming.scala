import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

val sparkConf = new SparkConf().setAppName("NetworkWordCount")
val ssc = new StreamingContext(sc, Seconds(1))

val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
val words = lines.flatMap(_.split(" "))
val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
wordCounts.saveAsTextFiles("../resources/output/files/")
ssc.start()
ssc.awaitTermination()
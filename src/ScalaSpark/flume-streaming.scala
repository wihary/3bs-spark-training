import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.flume._

val ssc = new StreamingContext(sc, Seconds(1))

val flumeStream = FlumeUtils.createStream(ssc, "localhost", 4242)
val lines = flumeStream.map(x => new String(x.event.getBody().array(), "UTF-8"))

lines.print()

ssc.start()
ssc.awaitTermination()
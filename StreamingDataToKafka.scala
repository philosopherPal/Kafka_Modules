import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object StreamingDataToKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafaDemo")

    val ssc = new StreamingContext(conf, Seconds(1))
    val rdd = ssc.socketTextStream("localhost",9999)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092", //kafka cluster address
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group", //consumer group name
      "auto.offset.reset" -> "latest", //latest automatically resets the offset to the latest offset
      "enable.auto.commit" -> (false: java.lang.Boolean)) // If true, the consumer's offset will be automatically submitted in the background
    val topics = Array("input") // consumption theme, you can consume multiple at the same time
    // Create a DStream, return the received input data
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD(f => {
      if (f.count > 0)
        f.foreach(f => println('\n'+f.value()+'\n'))
    })
    ssc.start();
    ssc.awaitTermination();
  }
}
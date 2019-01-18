import java.util.Properties

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object avgSpeed {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDemo")
    //刷新时间设置为1秒
    val ssc = new StreamingContext(conf, Seconds(1))
    //消费者配置
    val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "127.0.0.1:9092", //kafka集群地址
      "bootstrap.servers" -> "127.0.0.1:9092", //kafka cluster address
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group", //consumer group name
      "auto.offset.reset" -> "latest", //latest automatically resets the offset to the latest offset
      "enable.auto.commit" -> (false: java.lang.Boolean)) // If true, the consumer's offset will be automatically submitted in the background

    val topics = Array("test")

    val properties = new Properties()
    properties.put("bootstrap.servers", "127.0.0.1:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val outputTopic="adms"
    val producer = new KafkaProducer[String, String](properties)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))


    stream.foreachRDD(rdd => {
      if (rdd.count > 0) {
        val speedRdd = rdd.map(f => (1, f.value().split(',')(5).toDouble))
        val speedSum = speedRdd.reduceByKey( _+_ ).map(x => x._2).collect()
        val count = speedRdd.count()
        val avg = (speedSum(0) / count).toString
        println("\n" + avg + "\n")
        val message = new ProducerRecord[String, String](outputTopic, avg)
        producer.send(message).get().toString
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

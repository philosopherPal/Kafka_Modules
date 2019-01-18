import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

object wordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, "/Users/pallavitaneja/Desktop/spark_jaas.conf")
      //"/opt/kafka/kafka_jaas.conf")
      //"/Users/pallavitaneja/Desktop/spark_jaas.conf")
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafaDemo")
      //.set("spark.executor.extraJavaOptions","-Djava.security.auth.login.config = /Users/pallavitaneja/Desktop/spark_jaas.conf")
    // Refresh time is set to 1 second
    val ssc = new StreamingContext(conf, Seconds(1))
    // Consumer configuration
    //val client =
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "dsicloud2.usc.edu:9093", //kafka cluster address
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "devgrp", //consumer group name
      "auto.offset.reset" -> "latest", //latest automatically resets the offset to the latest offset
      "security.protocol" -> "SASL_SSL",
      "client.id" -> "kafka-dev",
      "sasl.kerberos.service.name" -> "kafka",
      "enable.auto.commit" -> (false: java.lang.Boolean)) // If true, the consumer's offset will be automatically submitted in the background
    val topics = Array("adms.prod.highway.realtime") // consumption theme, you can consume multiple at the same time
    // Create a DStream, return the received input data
    val messages = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    messages.count().print()
//    val stream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams))
//    // Print the acquired data, because 1 second refresh, so the data length is greater than 0 to print
//      stream.foreachRDD(f => {
//      if (f.count > 0)
//        f.foreach(f => println('\n'+f.value()+'\n'))
//    })
    ssc.start();
  }
}

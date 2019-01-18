import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * This object just populates the input kafka topic
  */
object SendDataToKafka extends App{

  val inputTopic = "newInput"
  val broker = "localhost:9092"

  val properties = new Properties()
  properties.put("bootstrap.servers", broker)
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](properties)
  val message =" ********************Data Received***************"
  var data = 0
  while (data < 2000) {
    data = data + 1
    val record = new ProducerRecord[String, String](inputTopic, (message + s" $data"))
    producer.send(record).get().toString
    println(s"inserted data : $data ")
  }
  producer.close()
}

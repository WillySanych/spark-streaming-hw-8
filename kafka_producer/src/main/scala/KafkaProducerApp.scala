import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import scala.util.Random
import scala.util.Random.nextDouble

object KafkaProducerApp extends App {

  val log = Logger.getLogger("KafkaProducerApp")

  val brokers = "localhost:9092"
  val topic = "records"

  private val properties = {
    val properties = new Properties()

    properties.put(BOOTSTRAP_SERVERS_CONFIG, brokers)
    properties.put(ACKS_CONFIG, "all")
    properties.put(RETRIES_CONFIG, 0)
    properties.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    properties
  }

  val producer = new KafkaProducer[Long, String](properties)

  var key = 0L

  while (true) {
    val timestamp = Instant.now.toEpochMilli
    key += 1
    val value = accountsOps
    val record = new ProducerRecord(topic, null, timestamp, key, value)

    producer.send(record)

    log.info(s"$record")

    TimeUnit.SECONDS.sleep(Random.nextInt(3) + 1)
  }

  def randomBetween(minInclusive: Double, maxExclusive: Double): Double = {
    require(minInclusive < maxExclusive, "Invalid bounds")

    val next = nextDouble() * (maxExclusive - minInclusive) + minInclusive
    if (next < maxExclusive) next
    else Math.nextAfter(maxExclusive, Double.NegativeInfinity)
  }

  def accountsOps (): String = {
    s"""
       {
         "sensor1": ${randomBetween(15, 30)},
         "sensor2": ${randomBetween(18, 27)},
         "sensor3": ${randomBetween(12, 26)},
         "sensor4": ${randomBetween(10, 24)},
         "sensor5": ${randomBetween(16, 28)}
       }
    """
  }
}

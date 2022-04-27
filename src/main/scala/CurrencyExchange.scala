
object CurrencyExchange extends App {

  import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
  import org.apache.kafka.streams.scala.StreamsBuilder
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._
  import org.apache.kafka.common.serialization.Serdes
  import org.apache.kafka.streams.KafkaStreams
  import org.apache.kafka.streams.StreamsConfig
  import java.util.Properties

  val builder = new StreamsBuilder

  val amounts: KStream[String, String] = builder.stream[String, String]("amounts")
  val rates: KTable[String, String] = builder.table[String, String]("rates")

  val out = amounts.join(rates) { (amount, rate) => s"Amount after exchange is ${amount.toDouble * rate.toDouble} for amount $amount & rate $rate" }
  out.to("out")
  println(out)

  val topology = builder.build
  println(topology.describe)

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "CurrencyExchangeAPP_v.2")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ":9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

  val streamsConfig = new StreamsConfig(props)

  val stream = new KafkaStreams(topology, props)
  stream.start()
}

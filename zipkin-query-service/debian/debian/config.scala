import com.twitter.logging.Level
import com.twitter.zipkin.builder.QueryServiceBuilder
import com.twitter.zipkin.elastic
import com.twitter.zipkin.elastic.{ElasticSpanStore, Common}
import com.twitter.zipkin.storage.Store


object ts_traits {
  def default_us_extractor = (ts:String) => {
    ts.substring(ts.size - 4, ts.size - 1).toLong
  }
  def default_ts_filter = (ts:String) => {
    ts.substring(0, ts.size - 4)
  }
}


val el = new Common(
  cluster_name = "ape-test-cloud",
  index_format = "'cocaine-v0.12-'yyyy.MM.dd-HH",
  es_connection_string = "elasticsearch://elastic01d.tst.ape.yandex.net:9300,elastic02d.tst.ape.yandex.net:9300,elastic03d.tst.ape.yandex.net:9300",
  timestamp_field = "@timestamp",
  trace_id_field = "trace_id",
  span_id_field = "span_id",
  parent_id_field = "parent_id",
  span_name_field = "rpc_name",
  service_name_field = "service",
  real_timestamp_field = "real_timestamp",
  message_field = "@message",
  ts_format_string = "yyyy-MM-dd'T'HH:mm:ss.SSS",
  timestamp_us_extractor = ts_traits.default_us_extractor,
  timestamp_filter = ts_traits.default_ts_filter
)

val elasticStore = new ElasticSpanStore(el)

QueryServiceBuilder(spanStore = elasticStore)
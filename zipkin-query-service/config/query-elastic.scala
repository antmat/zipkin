import com.twitter.logging.Level
import com.twitter.zipkin.builder.QueryServiceBuilder
import com.twitter.zipkin.elastic
import com.twitter.zipkin.elastic.Common
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
  index_format = "'test12ape-rsyslog-'yyyy-MM-dd",
  es_connection_string = "elasticsearch://elastic01d.tst.ape:9300,elastic02d.tst.ape:9300,elastic03d.tst.ape:9300",
  timestamp_field = "@timestamp",
  trace_id_field = "trace_id",
  span_id_field = "span_id",
  parent_id_field = "parent_id",
  span_name_field = "rpc_name",
  service_name_field = "service_name",
  real_timestamp_field = "real_timestamp",
  message_field = "@message",
  ts_format_string = "y-M-d'T'H:m:s.S",
  timestamp_us_extractor = ts_traits.default_us_extractor,
  timestamp_filter = ts_traits.default_ts_filter
)

val storeBuilder = Store.Builder(
  elastic.StorageBuilder(el),
  elastic.IndexBuilder(el)
)

QueryServiceBuilder(storeBuilder)
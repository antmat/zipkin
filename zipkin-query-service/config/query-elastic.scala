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
  "ape-test-cloud",
  "'test12ape-rsyslog-'yyyy-MM-dd",
  "elastic01d.tst.ape",
  9300,
  "/var/log/zipkin-query-service/query-service.log",
  Some(Level.ALL),
  "@timestamp",
  "trace_id",
  "span_id",
  "parent_id",
  "rpc_name",
  "service_name",
  "real_timestamp",
  "@message",
  "y-M-d'T'H:m:s.S",
  ts_traits.default_us_extractor,
  ts_traits.default_ts_filter
)

val storeBuilder = Store.Builder(
  elastic.StorageBuilder(el),
  elastic.IndexBuilder(el)
)

QueryServiceBuilder(storeBuilder)
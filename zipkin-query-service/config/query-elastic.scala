
import com.twitter.zipkin.builder.QueryServiceBuilder
import com.twitter.zipkin.elastic
import com.twitter.zipkin.storage.Store

val storeBuilder = Store.Builder(
  elastic.StorageBuilder("elasticsearch_antmat", "'logstash-'yyyy.MM.dd", "127.0.0.1", 9301),
  elastic.IndexBuilder("elasticsearch_antmat", "'logstash-'yyyy.MM.dd", "127.0.0.1", 9301)
)

QueryServiceBuilder(storeBuilder)


import com.twitter.zipkin.builder.QueryServiceBuilder
import com.twitter.zipkin.elastic
import com.twitter.zipkin.storage.Store

val storeBuilder = Store.Builder(
  elastic.StorageBuilder(),
  elastic.IndexBuilder()
)

QueryServiceBuilder(storeBuilder)

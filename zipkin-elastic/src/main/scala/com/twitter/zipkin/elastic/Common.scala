package com.twitter.zipkin.elastic

import java.math.BigInteger
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.{util, math}
import java.util.{TimeZone, Calendar, Date}

import com.sksamuel.elastic4s.{SearchType, IndexesTypes, ElasticsearchClientUri, ElasticClient}
import com.sksamuel.elastic4s.ElasticDsl._
import com.twitter.logging._
import com.twitter.util.{Await, Future, Promise, Throw}
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.Aggregation
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.collection.mutable.{HashSet, Set, Map, HashMap}
import scala.collection.immutable.List
import scala.concurrent.{Future => ScalaFuture, Promise => ScalaPromise, duration, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object ts_traits {
  def default_us_extractor = (ts:String) => {
    ts.substring(ts.length - 3).toLong
  }
  def default_ts_filter = (ts:String) => {
    ts.substring(0, ts.length - 3)
  }
}

object trace_traits {
  def default_id_parser = (id_str: String) => {
    if(id_str.length <16 || id_str.charAt(0) <'8') {
      java.lang.Long.parseLong(id_str, 16)
    } else {
      (java.lang.Long.parseLong(id_str.substring(0, 8) , 16) << 32) |
      (java.lang.Long.parseLong(id_str.substring(8, 16),  16) << 0)
    }
  }
  def default_id_generator = (id:Long) => {
    id.toHexString
  }
}

class Common (
               cluster_name: String,
               index_format: String,
               es_connection_string: String,
               val timestamp_field: String = "timestamp",
               val trace_id_field: String = "trace_id",
               val span_id_field: String = "span_id",
               val parent_id_field: String = "parent_id",
               val span_name_field: String = "span_name",
               val service_name_field: String = "service_name",
               val real_timestamp_field: String = "real_timestamp",
               val message_field: String = "message",
               val ts_format_string: String = "yyyy-MM-dd'T'HH:mm:ss.SSS",
               val timestamp_us_extractor: (String) => Long = ts_traits.default_us_extractor,
               val timestamp_filter: (String) => String = ts_traits.default_ts_filter,
               val id_parser: (String) => Long = trace_traits.default_id_parser,
               val id_generator: (Long) => String = trace_traits.default_id_generator,
               val max_clause_count: Long = 1023,
               val prefetch_count: Long = 1023,
               val lookback_index_cnt: Int = 12,
               val max_index_lookback: Int = 48,
               val cache_ttl_ms: Long = 3600000,
               val query_timeout_ms: Long = 60000,
               val max_result_count: Long = 1000,
               val elastic_hard_limit: Long = 50000,
               val min_span_per_trace: Int = 4,
               var span_per_trace: Int = 10
               ){
  val client: ElasticClient = ElasticClient.remote(
    ImmutableSettings.builder().put("cluster.name", cluster_name).build(),
    ElasticsearchClientUri(es_connection_string)
  )

  val log = Logger.get(getClass.getName)

  val index_formatter = new SimpleDateFormat(index_format)
  index_formatter.setTimeZone(TimeZone.getTimeZone("UTC"))

  val ts_formatter = new SimpleDateFormat(ts_format_string)
  ts_formatter.setTimeZone(TimeZone.getTimeZone("GMT"))

  var indexes: Set[String] = new HashSet[String]()
  var non_existing_indexes: Set[String] = new HashSet[String]()
  val cache: ServicesCache = new ServicesCache(this)

  implicit class ScalaFutureOps[A](sf: ScalaFuture[A]) {
    def asTwitter(): Future[A] = {
      val tp = new Promise[A]

      sf.onComplete {
        case Success(v) => tp.setValue(v)
        case Failure(t) => tp.setException(t)
      }

      tp
    }
  }

  def get_indexes(start: Int, end: Int, initial_time:Long = System.currentTimeMillis()): IndexesTypes  = {
    val idx_builder = new scala.collection.mutable.ListBuffer[String]
    val date = new Date()
    var step = start
    while(step < end || (idx_builder.isEmpty && step < max_index_lookback)) {
      date.setTime(initial_time - 3600000 * step)
      val idx_name = index_formatter.format(date)
      if(indexes.contains(idx_name)) {
        idx_builder += idx_name
      } else if(!non_existing_indexes.contains(idx_name)) {
        val f = client.execute({
          index exists (List(idx_name))
        })
        log.info("Waiting for query")
        val res = scala.concurrent.Await.result(f, Duration(30, SECONDS))
        log.info("Done")
        if(res.isExists) {
          indexes += idx_name
          idx_builder += idx_name
        } else if (step != 0) {
          non_existing_indexes += idx_name
          log.warning("Skipping non existing index:" + idx_name)
        } else {
          log.warning("Skipping non existing index:" + idx_name)
        }
      } else {
        log.warning("Skipping non existing index:" + idx_name)
      }
      step += 1
    }
    IndexesTypes(idx_builder.result(), Seq[String]())
  }

  def ts_convert(sh: SearchHit): Option[Long] = {
    val r_ts = sh.sourceAsMap().get("fields").asInstanceOf[java.util.HashMap[String, Object]].get(real_timestamp_field).asInstanceOf[Long]
    if(r_ts != 0) {
      Some(r_ts)
    }
    else {
      var ts = sh.sourceAsMap().get(timestamp_field).asInstanceOf[String];
      if(ts != null) {
        val us = timestamp_us_extractor(ts)
        ts = timestamp_filter(ts)
        Some(ts_formatter.parse(ts).getTime * 1000 + us.toLong)
      } else {
        None
      }
    }
  }

  def ts_format(ts: Long): String = {
    val simpleDateFormat = new SimpleDateFormat(ts_format_string)
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    simpleDateFormat.format(new Date(ts/1000))
  }

}

class ServicesCache(elastic: Common) {
  private var cache: Map[String, Seq[String]] = new HashMap[String, Seq[String]]
  private var cache_ts: Long = 0
  private val update_in_progress: AtomicBoolean = new AtomicBoolean(false)

  get_services().onSuccess(
    res =>
      elastic.log.info("Updated cache on startup. " + res.length.toString + " services")
  ).onFailure(
    ex =>
      elastic.log.info("Failed to update cache on startup: " + ex.getMessage)
  )

  def get_services(): Future[Seq[String]] = {
    elastic.log.info("get_services")
    val ret: Promise[Seq[String]] = new Promise[Seq[String]]
    ret.setValue(cache.keys.toSeq)

    val q = elastic.service_name_field + ":* AND " + elastic.trace_id_field + ":*"
    val cache_age = new Date().getTime - cache_ts
    elastic.log.info("Cache age: " + cache_age.toString, ", cache ttl:" + elastic.cache_ttl_ms.toString)
    if(cache_age > elastic.cache_ttl_ms) {
      if(update_in_progress.compareAndSet(false, true)) {
        elastic.log.info("Updating cache. getServiceNames.")
        val fut = elastic.ScalaFutureOps(elastic.client.execute(
        {
          search in elastic.get_indexes(0, elastic.lookback_index_cnt) query q aggs (
            agg terms elastic.service_name_field field elastic.service_name_field size 10000
            ) searchType SearchType.Count timeout FiniteDuration(elastic.query_timeout_ms, MILLISECONDS)
        }
        )).asTwitter().within(new com.twitter.util.JavaTimer, com.twitter.util.Duration(elastic.query_timeout_ms, MILLISECONDS))
        fut.onFailure(_ => update_in_progress.set(false))
        fut map {
          sr => {
            if (sr.getAggregations().get(elastic.service_name_field).isInstanceOf[StringTerms]) {
              val ag = sr.getAggregations().get(elastic.service_name_field).asInstanceOf[StringTerms]
              val counter: AtomicInteger = new AtomicInteger(ag.getBuckets.size())
              val new_cache = new HashMap[String, Seq[String]]
              elastic.log.info(counter.toString + " futures at all")
              var errored = false
              val ret = ag.getBuckets.asScala.map(
                b => {
                  val service = b.getKey.toLowerCase
                  elastic.log.info("Query " + service + " for span names")
                  val fut = update_and_get_span_names(service, new_cache).within(new com.twitter.util.JavaTimer, com.twitter.util.Duration(elastic.query_timeout_ms, MILLISECONDS)).respond(resp => {
                    resp match {
                      case Throw(throwable) => {
                        elastic.log.error("Error during span names query: " + throwable.getMessage)
                        errored = true
                      }
                      case _ =>
                    }
                    val cur = counter.decrementAndGet()
                    elastic.log.info(cur.toString + " futures pending")
                    if (cur == 0) {
                      if (!errored) {
                        cache_ts = new Date().getTime
                      }
                      cache = new_cache
                      update_in_progress.set(false)
                      elastic.log.info("cache updated")
                      log_cache()
                    }
                  })
                  Await.result(fut)
                  service
                }
              )
            }
            else {
              elastic.log.warning("Empty aggregation found")
              Seq.empty[String]
            }
          }
        }
      }
    }
    ret
  }

  def get_span_names(service:String): Future[Seq[String]] = {
    elastic.log.info("get_span_names for " + service)
    val respond = new Promise[Seq[String]]
    val result = cache.get(service)
    if(result.isDefined) {
      respond.setValue(result.get)
    } else {
      respond.setValue(Seq.empty)
    }
    if(new Date().getTime - cache_ts > elastic.cache_ttl_ms) {
      get_services()
    }
    respond
  }

  def log_cache() = {
    var s = ""
    cache.toSeq.foreach(
      pair => {
        var inner = ""
        pair._2.foreach(
          span_name => {
            if(inner != "") {
              inner += ", "
            }
            inner += span_name
          }
        )
        if(s != "") {
          s += "\n"
        }
        s += pair._1 + "->[" + inner + "]"
      }
    )
    elastic.log.info("Cache: " + s)
  }
  def update_and_get_span_names(service:String, new_cache: Map[String, Seq[String]]): Future[Seq[String]] = {
    elastic.log.info("update_and_get_span_names for " + service)
    val q = elastic.service_name_field + ":\"" + service + "\""
    elastic.log.info("Query: " + q)
    elastic.ScalaFutureOps(elastic.client.execute(
    {
      search in elastic.get_indexes(0, elastic.lookback_index_cnt) query q aggs(
        agg terms elastic.span_name_field field elastic.span_name_field
        ) searchType SearchType.Count timeout FiniteDuration(elastic.query_timeout_ms, MILLISECONDS)
    }
    ) ).asTwitter() map {
      sr => {
        elastic.log.warning("SR:" + sr.toString)
        val ag_obj: Aggregation = sr.getAggregations.get(elastic.span_name_field)
        if(ag_obj.isInstanceOf[StringTerms]) {
          val ag = ag_obj.asInstanceOf[StringTerms]
          val ret = ag.getBuckets.asScala.map(
            b => {
              elastic.log.info("B:" + b.getKey)
              b.getKey.toLowerCase
            }
          )
          new_cache.put(service, ret)
          ret
        } else {
          elastic.log.warning("Empty aggregation found")
          Seq.empty
        }
      }
    }
  }
}

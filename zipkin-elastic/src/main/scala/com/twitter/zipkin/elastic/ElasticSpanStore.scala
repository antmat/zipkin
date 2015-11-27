package com.twitter.zipkin.elastic

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.SearchType
import com.sksamuel.elastic4s.FieldSortDefinition
import com.sksamuel.elastic4s.TermsQueryDefinition
import com.twitter.util.{Promise, Future}
import com.twitter.zipkin.common.{Endpoint, Annotation, Span}
import com.twitter.zipkin.storage.{IndexedTraceId, QueryRequest, SpanStore}
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.xcontent.{ToXContent, XContentFactory, XContentBuilder}
import org.elasticsearch.search.aggregations.Aggregation
import org.elasticsearch.search.aggregations.bucket.terms.{Terms, StringTerms}
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats
import org.elasticsearch.search.aggregations.metrics.min.Min
import org.elasticsearch.search.sort.SortOrder
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{Map, HashMap, ListBuffer}
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

class Duration() {
  var min_ts:Long = 0
  var max_ts:Long = 0
  def apply(ts:Long) = {
    if (ts > max_ts || max_ts == 0) {
      max_ts = ts
    }
    if (ts < min_ts || min_ts == 0) {
      min_ts = ts
    }
  }
  def value() = {
    max_ts - min_ts
  }
}

class SpanGenerator(trace_id: Long, name: String, id: Long, parent_id: Option[Long]) {
  val duration: Duration = new Duration()
  val annotation_builder: ListBuffer[Annotation] = new ListBuffer[Annotation]

  def add_annotation(ann: Annotation) = {
    annotation_builder += ann
  }
  def add_ts(ts: Option[Long]) = {
    if(ts.isDefined) {
      duration(ts.get)
    }
  }

  def get(): Span = {
    new Span(
      trace_id,
      name,
      id,
      parent_id,
      Some(duration.min_ts),
      Some(duration.value()),
      annotation_builder.toList.sortBy(_.timestamp)
    )
  }
}

/**
  */
class ElasticSpanStore(elastic: Common)
extends SpanStore
{
  /**
   * Get the available trace information from the storage system.
   * Spans in trace are sorted by the first annotation timestamp
   * in that span. First event should be first in the spans list.
   *
   * <p/> Results are sorted in order of the first span's timestamp, and contain
   * up to [[QueryRequest.limit]] elements.
   */
  override def getTraces(qr: QueryRequest): Future[Seq[List[Span]]] = {
    var trace_ids = new mutable.HashSet[Long]()
    getTraces(qr, 0, 1, trace_ids)
  }

  private[this] def getTraces(qr: QueryRequest,  index_start: Int, index_end: Int, trace_ids: mutable.Set[Long]): Future[Seq[List[Span]]] = {
    var q: String = elastic.service_name_field+":\""+qr.serviceName + "\""
    q += " AND " + elastic.timestamp_field + ":[\"" + elastic.ts_format(qr.endTs - qr.lookback) + "\" TO \"" + elastic.ts_format(qr.endTs) + "\"]"

    if(qr.spanName.isDefined) {
      q += " AND " + elastic.span_name_field + ":" + qr.spanName.get
    }
    for(ann <- qr.annotations) {
      q += " AND " + elastic.message_field + ":" + ann
    }
    q += " AND " + elastic.trace_id_field + ":*"

    val indexes = elastic.get_indexes(index_start, index_end, qr.endTs/1000)

    var lim = 0
    var es_lim = 0
    if(qr.maxDuration.isDefined || qr.minDuration.isDefined || qr.limit*elastic.span_per_trace > elastic.elastic_hard_limit) {
      es_lim = elastic.elastic_hard_limit.toInt
      lim = (elastic.elastic_hard_limit/elastic.span_per_trace).toInt
    } else {
      lim = qr.limit
      es_lim = qr.limit*elastic.span_per_trace
    }

    elastic.log.info("getTraces. Preparing query. Indexes: " +  indexes +
      ", idx_start: " + index_start +
      ", index_end: " + index_end +
      ", query: " + q +
      ", limit:" + es_lim +
      ", start_ts: " + (qr.endTs - qr.lookback).toString +
      ", span: " + qr.spanName
    )

    elastic.ScalaFutureOps(elastic.client.execute(
      {
        search in indexes query q sort(
          new FieldSortDefinition(elastic.timestamp_field).order(SortOrder.DESC)
        ) limit(es_lim) timeout FiniteDuration(elastic.query_timeout_ms, MILLISECONDS)
      }
    ))
    .asTwitter().flatMap(
      sr => {
        elastic.log.info("getTraces. Query done.")
        for (hit <- sr.getHits.hits()) {
          val hit_data = hit.sourceAsMap().asScala
          val fields_obj = hit_data.get("fields")
          if (fields_obj.isDefined) {
            val fields = fields_obj.get.asInstanceOf[java.util.Map[String, Object]].asScala
            val trace_id_obj = fields.get(elastic.trace_id_field)
            if (trace_id_obj.isDefined && trace_ids.size < lim) {
              trace_ids += elastic.id_parser(trace_id_obj.get.asInstanceOf[String])
            }
          }
        }

        val msg = "Found " + trace_ids.size.toString + ", limit:" + lim + ", hits: " + sr.getHits.hits.length
        if(trace_ids.size < lim &&
            sr.getHits.hits().length < es_lim &&
            index_start < elastic.lookback_index_cnt
        ) {
          elastic.log.info(msg + ". Not enough results, query next index.")
          getTraces(qr, index_start+1, index_end+1, trace_ids)
        } else if(trace_ids.size < lim &&
            sr.getHits.hits.length == es_lim &&
            es_lim < elastic.elastic_hard_limit
        ) {
          elastic.span_per_trace *=2
          elastic.log.info(msg + ". Increase span_per_trace and requery.")
          getTraces(qr, index_start, index_end, trace_ids)
        } else {
          if(elastic.span_per_trace > elastic.min_span_per_trace) {
            elastic.span_per_trace -= 1
          }
          elastic.log.info(msg + ". Done. Quering traces by ids.")
          getTracesByIds(trace_ids.toList.slice(0, lim)) map(result => {
            result
              .filter(
                el => {
                  assert(el.head.duration.get >= 0L)
                  el.head.duration.get <= qr.maxDuration.getOrElse(Long.MaxValue) &&
                  el.head.duration.get >= qr.minDuration.getOrElse(0L)
                }
              )
              .sortWith(_.head.duration.getOrElse(0L) > _.head.duration.getOrElse(0L))
              .slice(0, math.min(elastic.max_result_count.toInt, qr.limit)
            )
          })
        }
      }
    )
  }

  /**
   * Get the available trace information from the storage system.
   * Spans in trace are sorted by the first annotation timestamp
   * in that span. First event should be first in the spans list.
   *
   * <p/> Results are sorted in order of the first span's timestamp, and contain
   * less elements than trace IDs when corresponding traces aren't available.
   */
  override def getTracesByIds(traceIds: Seq[Long]): Future[Seq[List[Span]]] = {
    elastic.log.info("getTracesByIds. id count:" + traceIds.size.toString)
    val storage: Map[Long, Map[Long, SpanGenerator]] = new HashMap[Long, Map[Long, SpanGenerator]]
    elastic.log.info("getTracesByIds. map created")
    getTracesByIds(traceIds, 0, 1, storage) map (_ => {
      val result = srToTraces(storage)
      elastic.log.info("getTracesById done. Result size:" + result.length.toString + ", storage size:" + storage.size.toString)
      result
    })
  }

  def getTracesByIds(traceIds: Seq[Long], index_start: Int, index_end: Int, storage: Map[Long, Map[Long, SpanGenerator]]): Future[Unit] = {
    var q = new ListBuffer[String]
    for (id <- traceIds) {
      q += elastic.id_generator(id)
    }
    //elastic.log.info("Q: " + q)
    elastic.log.info("getTracesByIds(" + index_start.toString + "," + index_end.toString + "). " +
      "traceIds.size: " + traceIds.size.toString + " Starting query.")
    val f = elastic.ScalaFutureOps(elastic.client.execute(
    {
//      search in elastic.get_indexes(index_start, index_end) query
//        termsQuery(elastic.trace_id_field, q:_*) limit
//        Int.MaxValue timeout
//        FiniteDuration(elastic.query_timeout_ms, MILLISECONDS)
      search in elastic.get_indexes(index_start, index_end) query "trace_id:*" postFilter
        termsFilter(elastic.trace_id_field, q:_*) limit
        Int.MaxValue timeout
        FiniteDuration(elastic.query_timeout_ms, MILLISECONDS)
    }
    )).asTwitter()
    f.flatMap(
      sr => {
        store_sr(sr, storage)
        val msg = "getTracesByIds("+index_start.toString+","+index_end.toString+"). Query done." +
          "traceIds.size: " + traceIds.size.toString + ", storage.size: " + storage.size.toString
        assert(storage.size <= traceIds.size)
        if (storage.size != (traceIds.size) && index_end < elastic.lookback_index_cnt) {
          elastic.log.info(msg + ", going to next index")
          getTracesByIds(traceIds, index_start + 1, index_end + 1, storage)
        } else {
          elastic.log.info(msg + "Fetched to storage " + storage.size.toString + " elements")
          val ret = new Promise[Unit]
          ret.setDone()
          ret
        }
      }
    )
  }

  private[this] def store_sr(sr: SearchResponse, storage: Map[Long, Map[Long, SpanGenerator]]) = {
    elastic.log.info("srToTraces")
    elastic.log.info("search hit count: " + sr.getHits.totalHits().toString)
    elastic.log.info("search hit real count: " + sr.getHits.hits().length.toString)
    val hits = sr.getHits.hits()
    elastic.log.info("hits loaded")
    for (hit <- hits) {
      //elastic.log.info("SH: " + hit.getSourceAsString)
      val hit_data = hit.sourceAsMap().asScala
      val fields_obj = hit_data.get("fields")
      if (fields_obj.isDefined) {
        val fields = fields_obj.get.asInstanceOf[java.util.Map[String, Object]].asScala
        val trace_id_obj = fields.get(elastic.trace_id_field)
        val span_id_obj = fields.get(elastic.span_id_field)
        val parent_id_obj = fields.get(elastic.parent_id_field)
        val ts = elastic.ts_convert(hit)

        val service_name = fields.getOrElse(elastic.service_name_field, "<unknown>").asInstanceOf[String]
        val message = hit_data.getOrElse(elastic.message_field, "<unknown>").asInstanceOf[String]
        val span_name = fields.getOrElse(elastic.span_name_field, "<unknown>").asInstanceOf[String]
        if (trace_id_obj.isDefined && span_id_obj.isDefined) {
          val trace_id = elastic.id_parser(trace_id_obj.get.asInstanceOf[String])
          val span_id = elastic.id_parser(span_id_obj.get.asInstanceOf[String])
          var parent_id: Option[Long] = None
          if(parent_id_obj.isDefined) {
            val v: Long = elastic.id_parser(parent_id_obj.get.asInstanceOf[String])
            if(v != 0) {
              parent_id = Some(v)
            }
          }

          val spans = storage.getOrElseUpdate(trace_id, new HashMap[Long, SpanGenerator])
          val span_gen = spans.getOrElseUpdate(span_id,
            new SpanGenerator(
              trace_id,
              span_name.toLowerCase,
              span_id,
              parent_id
            )
          )
          span_gen.add_annotation(new Annotation(
            ts.getOrElse(0),
            message,
            Some(Endpoint(0, 0, service_name))
          ))
          span_gen.add_ts(ts)
        } else {
          elastic.log.warning("Skipping search hit: " + hit.getSourceAsString + ". trace_id or span_id are not defined.")
        }
      } else {
        elastic.log.warning("Skipping search hit: " + hit.getSourceAsString + ". fields are not defined.")
      }
    }
  }

  private[this] def srToTraces(storage: Map[Long, Map[Long, SpanGenerator]]): Seq[List[Span]] = {
    storage.toSeq.map(
      trace_pair => {
        trace_pair._2.toList.map(
          span_gen_pair => {
            span_gen_pair._2.get
          }
        ).sortWith(_.timestamp.get < _.timestamp.get)
      }
    ).sortWith(_.head.timestamp.get < _.head.timestamp.get)
  }


  /**
   * Store a list of spans, indexing as necessary.
   *
   * <p/> Spans may come in sparse, for example apply may be called multiple times
   * with a span with the same id, containing different annotations. The
   * implementation should ensure these are merged at query time.
   */
  override def apply(spans: Seq[Span]): Future[Unit] = {
//    elastic.log.error("UNIMPLEMENTED!!!")
//    for(span <- spans) {
//      elastic.log.warning("Trying to store span: " + span.toString)
//    }
    val p:Promise[Unit] = new Promise[Unit]()
    p.setDone()
    p
  }

  override def getSpanNames(service: String): Future[Seq[String]] = {
    elastic.cache.get_span_names(service)
  }

  /**
   * Close writes and await possible draining of internal queues.
   */
  override def close(): Unit = {
    elastic.client.close()
  }

  /**
   * Get all the service names for as far back as the ttl allows.
   *
   * <p/> Results are sorted lexicographically
   */
  override def getAllServiceNames(): Future[Seq[String]] = {
    elastic.cache.get_services()
  }
}
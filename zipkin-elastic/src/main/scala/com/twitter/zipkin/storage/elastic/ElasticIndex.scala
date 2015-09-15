/*
 * Copyright 2012 Tumblr Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.zipkin.storage.elastic

import com.sksamuel.elastic4s.{SearchType, Executable, ElasticClient}
import com.sksamuel.elastic4s.ElasticDsl._
import com.twitter.logging.Logger

import com.twitter.util._
import com.twitter.zipkin.common.{AnnotationType, BinaryAnnotation, Span}
import com.twitter.zipkin.elastic.Common
import com.twitter.zipkin.storage.{TraceIdDuration, IndexedTraceId, Index}
import java.nio.ByteBuffer
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.bucket.terms.{StringTerms, Terms}
import org.elasticsearch.search.aggregations.metrics.min.Min

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

import scala.concurrent.{Future => ScalaFuture, Promise => ScalaPromise, ExecutionContext}
import scala.util.{Success, Failure}
import scala.collection.GenTraversableOnce
import scala.collection.JavaConverters._
import scala.NotImplementedError
import org.elasticsearch.common.settings

trait ElasticIndex extends Index {

  val elastic: Common

  override def close() = {
    elastic.client.close()
  }

  override def getTraceIdsByName(serviceName: String, span: Option[String],
                                 endTs: Long, limit: Int): Future[Seq[IndexedTraceId]] = {
    elastic.log.debug("getTraceIdsByName: " + serviceName + ", span: " + span + ", limit: " + limit.toString);

    elastic.ScalaFutureOps(elastic.client.execute(
    {
      search in elastic.get_index(endTs) query
        elastic.service_name_field+":"+serviceName postFilter
        rangeFilter(elastic.timestamp_field).lte(elastic.ts_format(endTs)) sort
        (by field elastic.timestamp_field) aggs(
        agg terms elastic.trace_id_field field elastic.trace_id_field size (limit)
        aggs(
          agg min "ts" field elastic.timestamp_field
          )
        ) searchType SearchType.Count
    }
    ) ).asTwitter(elastic.ec) map {
      sr => {
        val ag = sr.getAggregations().get(elastic.trace_id_field).asInstanceOf[Terms]
        ag.getBuckets.asScala.map (
          b => {
            //1.5 version
            val t_id = IndexedTraceId(elastic.id_parser(b.getKeyAsText.string()), b.getAggregations().get("ts").asInstanceOf[Min].value().toLong)

            //1.4 version
//            val t_id = IndexedTraceId(b.getKeyAsNumber.longValue(), b.getAggregations().get("ts").asInstanceOf[Min].getValue().toLong)
            elastic.log.debug("TraceID:" + t_id.timestamp + "," + t_id.traceId)
            t_id
          }
        )
      }
    }
  }
  override def getTraceIdsByAnnotation(serviceName: String, annotation: String, value: Option[ByteBuffer],
                                       endTs: Long, limit: Int): Future[Seq[IndexedTraceId]] = {

    elastic.log.debug("getTraceIdsByAnnotation: " + serviceName + ", annotation: " + annotation + ", limit: " + limit.toString);

    elastic.ScalaFutureOps(elastic.client.execute(
    {
      search in elastic.get_index(endTs) query
        elastic.message_field + ":" + annotation postFilter
        rangeFilter(elastic.timestamp_field).lte(elastic.ts_format(endTs)) postFilter
        queryFilter(query (elastic.service_name_field+":"+serviceName)) sort
        (by field elastic.timestamp_field) aggs(
        agg terms elastic.trace_id_field field elastic.trace_id_field size (limit)
          aggs(
          agg min "ts" field elastic.timestamp_field
          )
        ) searchType SearchType.Count
    }
    ) ).asTwitter(elastic.ec) map {
      sr => {
        val ag = sr.getAggregations().get(elastic.trace_id_field).asInstanceOf[Terms]
        ag.getBuckets.asScala.map (
          b => {
            //1.5 version
            val t_id = IndexedTraceId(elastic.id_parser(b.getKeyAsText.string()), b.getAggregations().get("ts").asInstanceOf[Min].value().toLong)

            //1.4 version
            //            val t_id = IndexedTraceId(b.getKeyAsNumber.longValue(), b.getAggregations().get("ts").asInstanceOf[Min].getValue().toLong)
            elastic.log.debug("TraceID:" + t_id.timestamp + "," + t_id.traceId)
            t_id
          }
        )
      }
    }
  }

  override def getTracesDuration(traceIds: Seq[Long]): Future[Seq[TraceIdDuration]] = {
    throw new NotImplementedError
    elastic.log.debug("getTracesDuration")
    elastic.ScalaFutureOps(elastic.client.execute(
      { search in elastic.get_index() query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            TraceIdDuration(
              sh.field("trace_id").getValue[Long](),
              sh.field("duration").getValue[Long](),
              elastic.ts_convert(sh)
            )
        )
    }
  }

  override def getServiceNames: Future[Set[String]] = {
    elastic.log.debug("getServiceNames")
    elastic.ScalaFutureOps(elastic.client.execute(
    {
      search in elastic.get_index() query elastic.service_name_field + ":*" aggs(
          agg terms elastic.service_name_field field elastic.service_name_field
        ) searchType SearchType.Count
    }
    ) ).asTwitter(elastic.ec) map {
      sr => {
        elastic.log.debug("SR:" + sr.toString)
        if(sr.getAggregations().get(elastic.service_name_field).isInstanceOf[StringTerms]) {
          val ag = sr.getAggregations().get(elastic.service_name_field).asInstanceOf[StringTerms]
          ag.getBuckets.asScala.map(
            b => {
              elastic.log.debug("B:" + b.getKey)
              b.getKey
            }
          ).toSet
        }
        else {
          elastic.log.debug("Empty aggregation found")
          Set.empty[String]
        }
      }
    }
  }

  override def getSpanNames(service: String): Future[Set[String]] = {
    elastic.log.debug("getSpanNames")
    elastic.ScalaFutureOps(elastic.client.execute(
    {
      search in elastic.get_index() query elastic.span_name_field + ":*" aggs(
        agg terms elastic.span_name_field field elastic.span_name_field
        ) searchType SearchType.Count
    }
    ) ).asTwitter(elastic.ec) map {
      sr => {
        elastic.log.debug("SR:" + sr.toString)
        val ag = sr.getAggregations().get(elastic.span_name_field).asInstanceOf[StringTerms]
        ag.getBuckets.asScala.map (
          b => {
            elastic.log.debug("B:" + b.getKey)
            b.getKey
          }
        ).toSet
      }
    }
  }

  override def indexTraceIdByServiceAndName(span: Span) : Future[Unit] = {
    throw new NotImplementedError

    elastic.log.debug("indexTraceIdByServiceAndName")
    elastic.ScalaFutureOps(elastic.client.execute(
    { search in "" query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            IndexedTraceId(sh.field("trace_id").getValue[Long](), elastic.ts_convert(sh))
        )
    }
  }

  override def indexSpanByAnnotations(span: Span) : Future[Unit] = {
    throw new NotImplementedError
    elastic.log.debug("indexSpanByAnnotations")
    elastic.ScalaFutureOps(elastic.client.execute(
    { search in "" query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            IndexedTraceId(sh.field("trace_id").getValue[Long](), elastic.ts_convert(sh))
        )
    }
  }

  override def indexServiceName(span: Span): Future[Unit] = {
    throw new NotImplementedError
    elastic.log.debug("indexServiceName")
    elastic.ScalaFutureOps(elastic.client.execute(
    { search in "" query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            IndexedTraceId(sh.field("trace_id").getValue[Long](), elastic.ts_convert(sh))
        )
    }
  }

  override def indexSpanNameByService(span: Span): Future[Unit] = {
    throw new NotImplementedError
    elastic.log.debug("indexSpanNameByService")
    elastic.ScalaFutureOps(elastic.client.execute(
    { search in "" query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            IndexedTraceId(sh.field("trace_id").getValue[Long](), elastic.ts_convert(sh))
        )
    }
  }

  override def indexSpanDuration(span: Span): Future[Unit] = {
    throw new NotImplementedError
    elastic.log.debug("indexSpanDuration")
    elastic.ScalaFutureOps(elastic.client.execute(
    { search in "" query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            IndexedTraceId(sh.field("trace_id").getValue[Long](), elastic.ts_convert(sh))
        )
    }
  }

}

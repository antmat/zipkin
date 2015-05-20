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
package com.twitter.zipkin.elastic

import com.twitter.conversions.time._
import com.twitter.util.Duration
import com.twitter.zipkin.builder.Builder
import com.twitter.zipkin.storage.Storage
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.zipkin.storage.elastic.{Common, ElasticStorage}

case class StorageBuilder(cluster_name: String,
                          index_format: String,
                          host: String,
                          port: Int) extends Builder[Storage] { self =>

  def apply() = {
    val el = new Common(cluster_name, index_format, host, port)
    Await.result(Future.value(new ElasticStorage { val elastic = el}), 10.seconds)
  }
}

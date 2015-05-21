import NativePackagerHelper._

enablePlugins(JavaServerAppPackaging)

javaOptions in Universal ++= Seq(
  "-zipkin.web.port=:8888",
  "-log.append=true",
  "-log.output=/var/log/zipkin-web/zipkin-web.log",
  "-zipkin.web.resourcesRoot=/usr/share/zipkin-web/resources/"
)

name := "zipkin-web"

version := "1.1.0"

maintainer := "Anton Matveenko <antmat@yandex-team.ru>"

packageSummary := "zipkin web interface package"

packageDescription := "Web interface for zipkin. Provides ability to analyze trace data of distributed system"

debianPackageDependencies := Seq.newBuilder[String].+=("zipkin-query-service").result()

mappings in Universal ++= directory("zipkin-web/src/main/resources/")
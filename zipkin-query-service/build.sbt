enablePlugins(LinuxPlugin)

enablePlugins(JavaServerAppPackaging)

javaOptions in Universal ++= Seq(
  "-f /etc/zipkin-query-service/query-elastic.scala"
)

name := "zipkin-query-service"

version := "1.1.0"

maintainer := "Anton Matveenko <antmat@yandex-team.ru>"

packageSummary := "zipkin query backend"

debianChangelog := Some(file("zipkin-query-service/changelog"))

packageDescription := "Zipkin backend to fetch span data"

debianPackageDependencies := Seq.newBuilder[String].+=("oracle-j2sdk1.7").result()

linuxPackageMappings in Debian += ( packageMapping(
  file("zipkin-query-service/config/query-elastic.scala") ->
  "/etc/zipkin-query-service/query-elastic.scala"
) withConfig("true"))

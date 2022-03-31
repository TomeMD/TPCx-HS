name := "TPCx-HS"
version := "1.0"
val hadoopVersion = "3.3.2"
val sparkVersion = "3.2.1"
val flinkVersion = "1.14.4"
crossScalaVersions := Seq("2.11.12", "2.12.13")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-mapreduce-examples" % hadoopVersion % "provided",
  "org.apache.spark" %% "spark-core"% sparkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-hadoop-compatibility" % flinkVersion % "provided",
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.commons.cli.**" -> "shadeApacheCommonsCLI.@1").inAll
)

assemblyJarName in assembly := s"${name.value}-master-${version.value}_${scalaBinaryVersion.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("org", "xmlpull", xs @ _*) => MergeStrategy.last
  case PathList("org", "xpp3", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

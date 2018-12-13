organization := "com.github.kimutansk"

name := "flink-ltsv"

// Follow Flink's scala version
scalaVersion := "2.11.12"
val flinkVersion = "1.7.0"
version:= "0.1.0_flink" + flinkVersion

resolvers += "Maven Central" at "http://central.maven.org/"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-table" % flinkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyOutputPath in assembly := file(s"target/${name.value}-${version.value}-sql.jar")
assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// When executing from test code or sbt run command, add extra dependency.
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run))

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-encoding", "UTF-8",
  "-explaintypes",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-optimize",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint:adapted-args",
  "-Xlint:nullary-unit",
  "-Xlint:inaccessible",
  "-Xlint:infer-any",
  "-Xlint:doc-detached",
  "-Xlint:private-shadow",
  "-Xlint:type-parameter-shadow",
  "-Xlint:poly-implicit-overload",
  "-Xlint:option-implicit",
  "-Xlint:delayedinit-select",
  "-Xlint:by-name-right-associative",
  "-Xlint:package-object-classes",
  "-Xlint:unsound-match",
  "-Xlint:stars-align",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused-import",
  "-Ywarn-unused")

scalastyleConfig := baseDirectory.value / "dev/scalastyle/scalastyle-config.xml"

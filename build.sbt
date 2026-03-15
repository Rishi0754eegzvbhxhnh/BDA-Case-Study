name         := "customer-segmentation"
version      := "1.0.0"
scalaVersion := "2.12.18"

javacOptions  ++= Seq("-source", "17", "-target", "17")
scalacOptions ++= Seq("-target", "jvm-17")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % "3.5.1",
  "org.apache.spark" %% "spark-sql"   % "3.5.1",
  "org.apache.spark" %% "spark-mllib" % "3.5.1"
)

fork := true

run / javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Dlog4j.rootCategory=WARN,console"
)

name := "livingwallet-spark"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "3.0.0",
                            "org.influxdb" % "influxdb-java" % "2.19",
                            "org.apache.spark" %% "spark-sql" % "3.0.0"
)

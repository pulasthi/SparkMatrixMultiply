name := "SparkMatrixMultiply"

version := "1.0"

scalaVersion := "2.11.8"

resolvers +=
  "Spark 1.0 RC" at "https://repository.apache.org/content/repositories/orgapachespark-1143"

resolvers +=
  "local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.6.1" % "provided"
)

libraryDependencies += "edu.indiana.soic.spidal" % "common" % "1.0"
libraryDependencies += "commons-cli" % "commons-cli" % "1.2"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
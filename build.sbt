lazy val root = (project in file(".")).
  settings(
    name := "Opinion-Mining",
    version := "1.0",
    scalaVersion := "2.11.8"
	
  )

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.0.1",
	"org.apache.spark"  %% "spark-mllib" % "2.0.1"
)

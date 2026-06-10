error id: file://<WORKSPACE>/laboratorio/build.sbt:
file://<WORKSPACE>/laboratorio/build.sbt
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -ThisBuild.
	 -ThisBuild#
	 -ThisBuild().
	 -scala/Predef.ThisBuild.
	 -scala/Predef.ThisBuild#
	 -scala/Predef.ThisBuild().
offset: 168
uri: file://<WORKSPACE>/laboratorio/build.sbt
text:
```scala
name := "reddit-ner-scala"

version := "0.1.0"

scalaVersion := "2.13.18"

ThisBuild / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
fork := true
This@@Build / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)


```


#### Short summary: 

empty definition using pc, found symbol in pc: 
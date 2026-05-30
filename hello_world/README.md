# Hello World - Spark Test

## Propósito

Este es un programa minimal de Spark diseñado para **verificar que tu instalación de Spark funciona correctamente** en tu máquina. Si encuentras problemas al ejecutar el programa principal del laboratorio, ejecuta este ejemplo primero para aislar si el problema está en tu ambiente o en el código del proyecto.

## Cómo correr el ejemplo

### Prerequisito: Apache Spark

Para instalar spark, solo es necesario agregarlo como una dependencia al archivo build.sbt.

## Configuración de Java para Spark con Scala/SBT
Actualmente, Apache Spark 3.x no presenta compatibilidad con versiones de Java de vanguardia como la 24 o 25. Al intentar ejecutar el framework bajo estos entornos, es común encontrarse con excepciones del tipo:

```
java.lang.UnsupportedOperationException: getSubject is not supported
```

o bien:

```
IllegalAccessError: cannot access class sun.nio.ch.DirectBuffer
```

Para subsanar esto, la recomendación técnica es emplear Java 17 durante la ejecución de Spark.

Cabe destacar que no resulta necesario prescindir de Java 24/25 ni alterar la configuración global del sistema operativo. Es factible preservar la versión más reciente como predeterminada y restringir el uso de Java 17 exclusivamente al ámbito de este proyecto.

### Linux (Ubuntu/Debian)

**1. Instalación de Java 17:**

```bash
sudo apt update
sudo apt install openjdk-17-jdk
```

Validar la instalación mediante:
```bash
java -version
```

**2. Ejecución con Java 17:**

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
PATH="$JAVA_HOME/bin:$PATH" \
SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" \
sbt run
```

Si el directorio varía según la distribución, pueden localizar el path correcto con:

```bash
update-alternatives --config java
```

o alternativamente:

```bash
readlink -f $(which java)
```

### macOS

**1. Instalación de Java 17:**

En sistemas con Homebrew, ejecuten:

```bash
brew install openjdk@17
```

Posteriormente, vinculen la instalación para que sea detectable por el sistema:

```bash
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
/Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

Para corroborar el éxito del proceso:

```bash
/usr/libexec/java_home -V
```

El listado debe incluir la versión 17.

**2. Ejecución con Java 17:**

Dentro del directorio del proyecto:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) \
PATH="$JAVA_HOME/bin:$PATH" \
SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" \
sbt run
```

Este procedimiento asegura que:

- Se emplee Java 17 únicamente para la tarea actual.
- Se mantenga la versión global del entorno sin alteraciones.
- Se incorporen los flags necesarios para evitar fallos internos en el manejo de memoria de Hadoop.

## Configuración sugerida en build.sbt

Para mitigar conflictos de classloaders y asegurar un cierre limpio de Spark, integren lo siguiente:

```scala
ThisBuild / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

fork := true

ThisBuild / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)
```

Estas opciones minimizan los problemas de interoperabilidad entre Spark, Hadoop y los runtimes de Java modernos.

Para evitar que sbt descargue todas las dependencias nuevamente, asegurarse de que el archivo build.sbt tenga exactamente las mismas versiones para Scala, Spark, sbt y las mismas librerías.
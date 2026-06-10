# Carpetas del proyecto.
  - En **hello-world** se encuentra un proyecto de prueba para corroborar que funcionen bien las dependencias.
  - En **reddit-mock** esta el servidor, este se tiene que levantar primero antes de correr el proyecto.
  - En **laboratorio** esta la carpeta principal del proyecto.

## pa ejecutar:

### con reddit remoto( o reddit normal )

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH="$JAVA_HOME/bin:$PATH" SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" sbt run
```

### con reddit-mock simulando servidor local

Abrir la terminal parado sobre la carpeta "reddit-mock" y ejecutar:
```bash
sbt compile
```
En otra terminal parado sobre "laboratorio" ejecutar:
```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH="$JAVA_HOME/bin:$PATH" SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" sbt "run --subscription-file data/local_subscriptions.json"
```

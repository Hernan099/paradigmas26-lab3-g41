# Explicación paso a paso del ejercicio 1_a

## Paso 1 — Leer suscripciones (`FileIO.readSubscriptions`)

**Qué hace:** Lee el archivo `subscriptions.json` del disco y lo parsea con json4s. Devuelve una lista de `Option[Subscription]`.

**Cómo lo identifiqué:** En `Main.scala:10` se llama a `FileIO.readSubscriptions(cmdArgs.subscriptionFile)`. Mirando `FileIO.scala:13-25`, la función abre el archivo, parsea el JSON completo, y mapea cada entrada a `Some(Subscription(name, url))`.

**Tipo de salida:** `List[Option[Subscription]]` — cada elemento es `Some(Subscription)` si la entrada del JSON tenía los campos `name` y `url`, o `None` si estaba malformada.

**Rol:** Este paso lo ejecuta el **driver**, porque implica lectura de un archivo de configuración local. No tiene sentido distribuirlo.

---

## Paso 2 — Filtrar suscripciones válidas (`.flatten`)

**Qué hace:** Descarta los `None` y extrae los `Subscription` válidos.

**Cómo lo identifiqué:** En `Main.scala:13`, se llama a `subscriptionOpts.flatten`, que convierte `List[Option[Subscription]]` en `List[Subscription]` eliminando los `None`.

**Tipo de salida:** `List[Subscription]` — lista "limpia" de suscripciones con `name: String` y `url: String`.

**Rol:** **Driver.** Es una operación trivial sobre una lista pequeña de configuración.

---

## Paso 3 — Descargar feeds (`FileIO.downloadFeed`)

**Qué hace:** Para cada `Subscription`, hace una petición HTTP a `subscription.url` y descarga el JSON del feed de Reddit.

**Cómo lo identifiqué:** En `Main.scala:16-20`, hay un `subscriptions.map { subscription => ... FileIO.downloadFeed(subscription.url) ... }`. Mirando `FileIO.scala:32-37`, la función usa `Source.fromURL(url)` para descargar el contenido.

**Tipo de salida:** `List[(Boolean, List[Post])]` — nótese que en el código actual, la descarga y el parseo están **acoplados** dentro del mismo `.map`. Cada tupla indica `(fueExitosa, postsExtraídos)`.

**Rol:** Este paso es **paralelizable**. Cada descarga es independiente de las demás. En Spark, cada URL podría ser procesada por un worker diferente.

---

## Paso 4 — Parsear JSON → Posts (`JsonParser.parsePosts`)

**Qué hace:** Toma el JSON descargado y extrae los posts de Reddit (título + selftext).

**Cómo lo identifiqué:** En `Main.scala:18`, dentro del mismo `.map`, se llama a `JsonParser.parsePosts(feedContent, subscription.name)`. En `JsonParser.scala:12-30`, la función parsea el JSON, navega hasta `data.children[].data` y extrae `title` y `selftext` para crear objetos `Post`.

**Tipo de salida:** `List[Post]` por cada feed (envuelto en la tupla del paso 3).

**Rol:** **Paralelizable.** Cada feed se parsea de forma independiente. En Spark, esto sería un `map` o `flatMap` sobre el RDD de feeds descargados.

---

## Paso 5 — Aplanar posts (`.flatMap`)

**Qué hace:** Junta todos los posts de todos los feeds en una sola lista plana.

**Cómo lo identifiqué:** `Main.scala:27` → `downloadResults.flatMap(_._2)`. Convierte `List[(Boolean, List[Post])]` en `List[Post]`.

**Tipo de salida:** `List[Post]` — todos los posts de todas las suscripciones en una lista única.

**Rol:** En Spark esto sería implícito (un `flatMap` ya distribuido), o lo haría el framework al unificar particiones.

---

## Paso 6 — Filtrar posts vacíos (`Analyzer.filterEmptyPosts`)

**Qué hace:** Elimina posts donde `title` o `selftext` estén vacíos o solo contengan whitespace.

**Cómo lo identifiqué:** `Main.scala:32` → `Analyzer.filterEmptyPosts(allPosts)`. En `Analyzer.scala:9-15`, la función filtra con `post.title.nonEmpty && post.selftext.nonEmpty && post.selftext.trim.nonEmpty`.

**Tipo de salida:** `List[Post]` — subconjunto de posts válidos (no vacíos).

**Rol:** **Paralelizable.** Es un `filter` puro: cada post se evalúa independientemente. En Spark → `rdd.filter(...)`.

---

## Paso 7 — Cargar diccionarios (`Dictionary.loadAll`)

**Qué hace:** Lee los archivos de texto (`people.txt`, `universities.txt`, `languages.txt`, etc.) y construye una lista de objetos `NamedEntity` (con su subtipo concreto: `Person`, `University`, `ProgrammingLanguage`, etc.).

**Cómo lo identifiqué:** `Main.scala:60` → `Dictionary.loadAll(cmdArgs.entitiesDir)`. En `Dictionary.scala:31-50`, se cargan 5 archivos y se concatenan las listas con `:::`.

**Tipo de salida:** `List[NamedEntity]` — diccionario completo con todas las entidades conocidas.

**Rol:** **Driver.** Este paso lee archivos locales de configuración/datos pequeños. En Spark, el diccionario se cargaría en el driver y se enviaría a los workers como **broadcast variable**.

> **Importante:** Este paso es **independiente** de los pasos 3-6 (descarga y filtrado de posts). Es la única bifurcación real del grafo de dependencias: ambas ramas (posts filtrados + diccionario) confluyen en el paso 8.

---

## Paso 8 — Detectar entidades (`Analyzer.detectEntities`)

**Qué hace:** Para cada post filtrado, combina título + selftext y busca qué entidades del diccionario aparecen como palabras completas (matching case-insensitive).

**Cómo lo identifiqué:** `Main.scala:63-66` → `filteredPosts.flatMap { post => Analyzer.detectEntities(combinedText, dictionary) }`. En `Analyzer.scala:25-30`, la función parte el texto en palabras, las pasa a minúsculas y filtra las entidades del diccionario cuyo `.text.toLowerCase` esté en el set de palabras.

**Tipo de salida:** `List[NamedEntity]` — lista plana de todas las entidades encontradas en todos los posts (con repeticiones si una entidad aparece en múltiples posts).

**Rol:** **Paralelizable.** Es el paso más pesado computacionalmente. Cada post se analiza de forma independiente contra el mismo diccionario. En Spark → `rdd.flatMap(post => detectEntities(post, broadcastDict.value))`.

---

## Paso 9 — Contar entidades (`Analyzer.countEntities`)

**Qué hace:** Agrupa las entidades por `(entityType, entityName)` y cuenta las ocurrencias de cada una.

**Cómo lo identifiqué:** `Main.scala:69` → `Analyzer.countEntities(allEntities)`. En `Analyzer.scala:37-43`, usa `.groupBy(e => (e.entityType, e.text)).mapValues(_.size)`.

**Tipo de salida:** `Map[(String, String), Int]` — mapa de `(tipo, nombre)` → cantidad de apariciones.

**Rol:** **Paralelizable (reducción).** En Spark, esto sería un `map(e => ((e.entityType, e.text), 1)).reduceByKey(_ + _)`. Es la etapa de **agregación/reducción** del patrón MapReduce.

---

## Paso 10 — Ranking top-K (`Formatters.formatEntityStats`)

**Qué hace:** Ordena las entidades por frecuencia (descendente), luego por tipo y nombre (alfabético), toma las top-K, y formatea la salida para consola.

**Cómo lo identifiqué:** `Main.scala:74` → `Formatters.formatEntityStats(entityCounts, cmdArgs.topK)`. En `Formatters.scala:51-65`, ordena con `.sortBy((-count, entityType, entityName)).take(topK)` y formatea cada línea.

**Tipo de salida:** `String` — texto formateado para imprimir en consola.

**Rol:** **Driver.** Este paso recolecta todos los datos en un solo nodo para ordenarlos y mostrar el resultado final. En Spark sería una **acción** (`collect` o `takeOrdered`).

---

## Resumen: Driver vs. Workers

| Paso | Descripción | Ejecuta | ¿Paralelizable? |
|------|-------------|---------|-----------------|
| 1 | Leer suscripciones | Driver | No (archivo local de config) |
| 2 | Filtrar suscripciones válidas | Driver | No (lista pequeña) |
| 3 | Descargar feeds | Workers | **Sí** (cada URL es independiente) |
| 4 | Parsear JSON → Posts | Workers | **Sí** (cada feed es independiente) |
| 5 | Aplanar posts | Framework | Implícito en Spark |
| 6 | Filtrar posts vacíos | Workers | **Sí** (cada post es independiente) |
| 7 | Cargar diccionarios | Driver | No (datos pequeños, se hace broadcast) |
| 8 | Detectar entidades | Workers | **Sí** (cada post es independiente) |
| 9 | Contar entidades | Workers+Driver | **Sí** (reduceByKey distribuido) |
| 10 | Ranking top-K | Driver | No (acción final, collect) |

# Explicación paso a paso — Ejercicio 1b

## ¿Cómo llegué a la clasificación de cada paso?

### Método general

Para clasificar cada paso, me hice tres preguntas sobre cada uno:

1. **¿Opera sobre datos distribuidos (RDD) o sobre datos locales del driver?** → Si es local, no encaja en ninguna abstracción de transformación.
2. **¿Cuántos resultados produce por cada elemento de entrada?** → Exactamente 1 = `map`, 0 o más = `flatMap`, descarta o mantiene = `filter`.
3. **¿Necesita ver todos los elementos para producir su resultado?** → Sí = `reduceByKey` u otra reducción. No = `map`/`flatMap`/`filter`.

---

### Paso 1 — Leer suscripciones → **No encaja (Driver)**

**Razonamiento:** Miré `Main.scala:10` → `FileIO.readSubscriptions(cmdArgs.subscriptionFile)`. Esta función lee UN archivo local del filesystem (`subscriptions.json`). No hay colección distribuida todavía — el programa recién arranca. 

**¿Por qué no es map/flatMap/reduceByKey?** Porque no transforma elementos de un RDD. Este paso ocurre *antes* de que exista cualquier RDD. Su resultado (`List[Option[Subscription]]`) se usa en el driver para *crear* el RDD inicial con `sc.parallelize(...)`.

**Conclusión:** Es código del driver, previo a la distribución.

---

### Paso 2 — Filtrar suscripciones válidas → **No encaja (Driver)**

**Razonamiento:** En `Main.scala:13` se llama a `subscriptionOpts.flatten`. Esto opera sobre la misma lista local del driver (las suscripciones son típicamente 5-20 elementos).

**¿Por qué no es filter?** Técnicamente `.flatten` sobre `List[Option[A]]` sí filtra los `None`, pero la lista es tan pequeña que distribuirla no tendría sentido. Sigue siendo código del driver antes de la creación del RDD.

**Conclusión:** Driver. El RDD aún no existe.

---

### Paso 3 — Descargar feeds → **map**

**Razonamiento:** Miré `Main.scala:16-20`. Para cada `Subscription`, se llama a `FileIO.downloadFeed(subscription.url)`. Me pregunté:

- **¿Cada elemento se procesa independientemente?** Sí. Descargar el feed de "programming" no afecta descargar el de "computerscience".
- **¿Cuántos resultados produce?** Exactamente 1 por cada suscripción: un `Option[String]` (el JSON descargado, o `None` si falla).
- **¿Necesita ver otros elementos?** No.

**¿Por qué map y no flatMap?** Porque cada suscripción produce exactamente UN resultado. No produce una cantidad variable. Una suscripción → un JSON (o un error).

**Conclusión:** `map` — transformación 1:1.

---

### Paso 4 — Parsear JSON → Posts → **flatMap**

**Razonamiento:** En `Main.scala:18` se llama a `JsonParser.parsePosts(json, name)`. Miré `JsonParser.scala:12-30` y vi que cada JSON de un feed contiene **múltiples posts** (está en `data.children[]`). 

- **¿Cada elemento se procesa independientemente?** Sí. Parsear un feed no afecta a otros.
- **¿Cuántos resultados produce?** **Variable**: un feed puede tener 0 posts (si el JSON está mal), 25 posts, o cualquier cantidad.
- **¿Necesita ver otros elementos?** No.

**¿Por qué flatMap y no map?** Porque `map` produciría `RDD[List[Post]]` (un RDD donde cada elemento es una lista). Lo que queremos es `RDD[Post]` (un RDD donde cada elemento es un post individual). `flatMap` "aplana" las listas: `1 feed → N posts`.

**Conclusión:** `flatMap` — transformación 1:N.

---

### Paso 5 — Aplanar posts → **Implícito en el flatMap anterior**

**Razonamiento:** En `Main.scala:27` se llama a `downloadResults.flatMap(_._2)`. Pero en Spark, este aplanamiento ya ocurriría como parte del `flatMap` del paso 4. Spark no distingue entre "parsear" y "aplanar": el `flatMap` hace ambas cosas.

**Conclusión:** No es un paso separado en Spark. Queda absorbido por el `flatMap` anterior.

---

### Paso 6 — Filtrar posts vacíos → **filter**

**Razonamiento:** En `Main.scala:32` se llama a `Analyzer.filterEmptyPosts(allPosts)`. Mirando `Analyzer.scala:9-15`:

```scala
posts.filter { post =>
  post.title.nonEmpty &&
  post.selftext.nonEmpty &&
  post.selftext.trim.nonEmpty
}
```

- **¿Cada elemento se procesa independientemente?** Sí. Que un post esté vacío no depende de otros posts.
- **¿Cuántos resultados produce?** 0 o 1 por cada post (lo mantiene o lo descarta). **No transforma** el elemento: el Post de entrada y el Post de salida son idénticos.
- **¿Necesita ver otros elementos?** No.

**¿Por qué no es map?** Porque `map` produce exactamente 1 resultado por entrada. `filter` puede producir 0 (descartando el elemento).

**¿Por qué no es flatMap?** Podría expresarse como `flatMap` técnicamente (`flatMap(post => if (condición) List(post) else List())`), pero semánticamente es un `filter`: no transforma el dato, solo decide si mantenerlo.

**¿Por qué no es reduceByKey?** Porque cada post se evalúa por sí solo, sin necesidad de ver otros.

**¿Encaja en map/flatMap/reduceByKey?** No exactamente. Es **`filter`**, que es una abstracción propia de Spark (una transformación lazy como `map` y `flatMap`, pero con semántica diferente). No encaja en las tres categorías que pide el enunciado.

---

### Paso 7 — Cargar diccionarios → **No encaja (Driver + broadcast)**

**Razonamiento:** En `Main.scala:60` se llama a `Dictionary.loadAll(cmdArgs.entitiesDir)`. Mirando `Dictionary.scala:31-50`, se leen 5 archivos `.txt` del filesystem local y se concatenan en una sola `List[NamedEntity]`.

**¿Por qué no opera sobre el RDD?** Porque los diccionarios son datos de referencia (lookup data), no datos a procesar. Son ~cientos de entradas. No tiene sentido distribuirlos como RDD.

**¿Cómo se haría en Spark?** Se cargaría en el driver con código normal de Scala, y luego se enviaría a todos los workers con `sc.broadcast(dictionary)`. Los workers lo leerían de la broadcast variable en el paso 8.

**Conclusión:** No encaja. Es un patrón de datos auxiliares del driver (broadcast pattern).

---

### Paso 8 — Detectar entidades → **flatMap**

**Razonamiento:** En `Main.scala:63-66`:

```scala
filteredPosts.flatMap { post =>
  val combinedText = post.title + " " + post.selftext
  Analyzer.detectEntities(combinedText, dictionary)
}
```

Mirando `Analyzer.scala:25-30`, para cada post se buscan las entidades del diccionario que aparecen como palabras en el texto.

- **¿Cada elemento se procesa independientemente?** Sí. Detectar entidades en un post no depende de otros posts.
- **¿Cuántos resultados produce?** **Variable**: un post puede contener 0 entidades (si ningún nombre del diccionario aparece en el texto), o varias (si menciona "Scala", "Google", "Stanford", etc.).
- **¿Necesita ver otros elementos?** No. Solo necesita el diccionario, que es una constante (broadcast).

**¿Por qué flatMap y no map?** Porque `map` daría `RDD[List[NamedEntity]]`. Queremos `RDD[NamedEntity]`. `flatMap` aplana: `1 post → N entidades`.

**Conclusión:** `flatMap` — transformación 1:N.

---

### Paso 9 — Contar entidades → **map + reduceByKey**

**Razonamiento:** En `Main.scala:69` se llama a `Analyzer.countEntities(allEntities)`. Mirando `Analyzer.scala:37-43`:

```scala
entities
  .groupBy(entity => (entity.entityType, entity.text))
  .view.mapValues(_.size).toMap
```

Esto agrupa por `(tipo, nombre)` y cuenta cuántas veces aparece cada uno.

- **¿Cada elemento se procesa independientemente?** **No.** Para contar "cuántas veces aparece Scala", necesito ver TODOS los elementos donde aparece "Scala".
- **¿Necesita ver múltiples elementos?** Sí — es una operación de **agregación**.

**¿Cómo se expresa en Spark?** En dos pasos:
1. `map(entity => ((entity.entityType, entity.text), 1))` — transforma cada entidad en un par clave-valor
2. `reduceByKey(_ + _)` — suma los valores para cada clave

**¿Por qué reduceByKey y no groupBy?** Porque `reduceByKey` es más eficiente en Spark: reduce localmente en cada partición antes del shuffle (combiner-side optimization), mientras que `groupBy` envía todos los valores al mismo nodo.

**Conclusión:** `map` + `reduceByKey` — patrón MapReduce clásico.

---

### Paso 10 — Ranking top-K → **No encaja (Acción)**

**Razonamiento:** En `Main.scala:74` se llama a `Formatters.formatEntityStats(entityCounts, cmdArgs.topK)`. Mirando `Formatters.scala:51-65`:

```scala
entityCounts.toList
  .sortBy { case ((entityType, entityName), count) => (-count, entityType, entityName) }
  .take(topK)
```

- **¿Puede procesarse elemento por elemento?** **No.** Para saber si una entidad está en el top-K, necesito compararla con TODAS las demás.
- **¿Es una transformación?** **No.** Es una **acción**: produce un resultado final que sale del mundo distribuido y va al driver para mostrarse en pantalla.

**¿Cómo se haría en Spark?** Con `rdd.takeOrdered(topK)(Ordering.by(...))` o con `rdd.collect()` seguido de un sort local. Ambas son **acciones** que disparan la ejecución del DAG completo.

**¿Por qué no es reduceByKey?** Porque `reduceByKey` produce otro RDD (transformación lazy). El ranking produce un resultado final que se lleva al driver. Además, `reduceByKey` opera sobre pares clave-valor, pero el ranking necesita un ordenamiento global, no una reducción por clave.

**Conclusión:** No encaja en map/flatMap/reduceByKey. Es una acción del driver.

---

## Resumen del mapeo

| Paso | Abstracción | Razón clave |
|------|-------------|-------------|
| 1 | No encaja | Pre-RDD, driver |
| 2 | No encaja | Pre-RDD, driver |
| 3 | `map` | 1 suscripción → 1 JSON (1:1) |
| 4 | `flatMap` | 1 feed → N posts (1:N) |
| 5 | (implícito) | Absorbido por flatMap del paso 4 |
| 6 | `filter` | Mantiene o descarta, no transforma (no es map/flatMap/reduceByKey) |
| 7 | No encaja | Datos auxiliares del driver (broadcast) |
| 8 | `flatMap` | 1 post → N entidades (1:N) |
| 9 | `map` + `reduceByKey` | Patrón MapReduce: generar pares + agregar |
| 10 | No encaja | Acción (collect/takeOrdered), no transformación |

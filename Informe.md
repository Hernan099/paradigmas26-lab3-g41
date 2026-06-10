# Informe — Laboratorio 3

## Ejercicio 1: Identificar las regiones paralelizables

### a_ Grafo de dependencias

____________________________________________________________________________________
| Arista |          Output de → Input de           |           Tipo Scala          |
|--------|-----------------------------------------|-------------------------------|
| 1 → 2  | Suscripciones leídas del JSON (pueden   | `List[Option[Subscription]]`  |
|        | ser `None` si malformadas)              |                               |
|--------|-----------------------------------------|-------------------------------|
| 2 → 3  | Solo las suscripciones válidas (se      | `List[Subscription]`          |
|        | descartaron los `None`)                 |                               |
|--------|-----------------------------------------|-------------------------------|
| 3 → 4  | Resultados de descarga: tupla (éxito?,  | `List[(Boolean, List[Post])]` |
|        | posts parseados)                        |                               |
|--------|-----------------------------------------|-------------------------------|
| 4 → 5  | Mismo tipo; la descarga y el parseo     | `List[(Boolean, List[Post])]` |
|        | ocurren dentro del mismo `map`          |                               |
|--------|-----------------------------------------|-------------------------------|
| 5 → 6  | Todos los posts de todos los feeds,     | `List[Post]`                  |
|        | aplanados en una sola lista             |                               |
|--------|-----------------------------------------|-------------------------------|
| 6 → 8  | Posts no vacíos, listos para análisis   | `List[Post]`                  |
|--------|-----------------------------------------|-------------------------------|
| 7 → 8  | Diccionario completo de entidades       | `List[NamedEntity]`           |
|--------|-----------------------------------------|-------------------------------|
| 8 → 9  | Todas las entidades detectadas en todos | `List[NamedEntity]`           |
|        | los posts                               |                               |
|--------|-----------------------------------------|-------------------------------|
| 9 → 10 | Conteo de cada entidad agrupada por     | `Map[(String, String), Int]`  |
|        | `(entityType, entityName)`              |                               |
|________|_________________________________________|_______________________________|

---

### b_ Mapeo de cada paso a abstracciones de Spark

______________________________________________________________________________________
| Paso |    Descripción       |   Abstracción Spark    |        Justificación        |
|------|----------------------|------------------------|-----------------------------|
|  1   | Leer suscripciones   | **No encaja** (Driver) | Es lectura de un archivo de |
|      |                      |                        |configuración local.El driver|
|      |                      |                        |lo ejecuta antes de crear el |
|      |                      |                        |RDD. No opera sobre datos    |
|      |                      |                        |distribuidos.                |
|------|----------------------|------------------------|-----------------------------|
|  2   | Filtrar suscripciones| **No encaja** (Driver) | Opera sobre la lista pequeña|
|      | válidas              |                        |de config en el driver.Podría|
|      |                      |                        |verse como `filter`, pero no |
|      |                      |                        |tiene sentido distribuirlo.  |
|------|----------------------|------------------------|-----------------------------|
|  3   | Descargar feeds      | **map**                | Cada suscripción se         |
|      |                      |                        |transforma independientemente|
|      |                      |                        |en exactamente un resultado  |
|      |                      |                        |(el JSON descargado o un     |
|      |                      |                        |error).`rdd.map(sub =>  `    |
|      |                      |                        |`downloadFeed(sub.url))`     |
|------|----------------------|------------------------|-----------------------------|
|  4   | Parsear JSON → Posts | **flatMap**            | Cada feed JSON produce una  |
|      |                      |                        |cantidad variable de posts (0|
|      |                      |                        |o más). `rdd.flatMap(json =>`|
|      |                      |                        |`parsePosts(json))`          |
|------|----------------------|------------------------|-----------------------------|
|  5   | Aplanar posts        | *(implícito en el      | En Spark, el `flatMap` del  |
|      |                      |  flatMap anterior)*    | paso 4 ya produce el        |
|      |                      |                        | aplanamiento. No es un paso |
|      |                      |                        | separado.                   |
|------|----------------------|------------------------|-----------------------------|
|  6   | Filtrar posts vacíos | **filter**             | Cada post se evalúa         |
|      |                      |                        | independientemente con un   |
|      |                      |                        | predicado booleano.         |
|      |                      |                        | `rdd.filter(post => `       |
|      |                      |                        | `post.title.nonEmpty && `   |
|      |                      |                        | `...`. Nota: `filter` no es |
|      |                      |                        | `map` ni `flatMap` ni       |
|      |                      |                        | `reduceByKey`, es su propia |
|      |                      |                        | Spark.                      |
|------|----------------------|------------------------|-----------------------------|
|  7   | Cargar diccionarios  | **No encaja** (Driver +| Es lectura de archivos      |
|      |                      | broadcast)             | pequeños de datos de        |
|      |                      |                        | referencia. Se carga en el  |
|      |                      |                        | driver y se distribuye como |
|      |                      |                        | broadcast variable. No opera|
|      |                      |                        | sobre el RDD.               |
|------|----------------------|------------------------|-----------------------------|
|  8   | Detectar entidades   | **flatMap**            | Cada post produce 0 o más   |
|      |                      |                        | entidades detectadas        |
|      |                      |                        | (cantidad variable).        |
|      |                      |                        | `rdd.flatMap(post =>`       |
|      |                      |                        | `detectEntities(post, `     |
|      |                      |                        | `dictBroadcast.value))`     |
|------|----------------------|------------------------|-----------------------------|
|  9   | Contar entidades     | **map + reduceByKey**  |Primero un `map` para generar|
|      |                      |                        | pares clave-valor:          |
|      |                      |                        |`rdd.map(e =>((e.entityType,`|
|      |                      |                        | `e.text), 1))`, luego un    |
|      |                      |                        | `reduceByKey(_ + _)` para   |
|      |                      |                        | sumar los conteos.          |
|------|----------------------|------------------------|-----------------------------|
|  10  | Ranking top-K        | **No encaja** (Driver) | Es una **acción**           |
|      |                      |                        | (`takeOrdered` o `collect`  |
|      |                      |                        | `+ sort`). Requiere         |
|      |                      |                        |recolectar datos en el driver|
|      |                      |                        |para ordenarlos globalmente y|
|      |                      |                        |mostrar el resultado. No es  |
|      |                      |                        |una transformación lazy.     |
|______|______________________|________________________|_____________________________|

---

### Pasos que NO encajan en map/flatMap/reduceByKey

**Paso 1 (Leer suscripciones) y Paso 2 (Filtrar suscripciones válidas):**
No encajan porque operan _antes_ de que exista un RDD. Son pasos de inicialización que el driver ejecuta secuencialmente para obtener la lista de URLs a procesar. El resultado de estos pasos se usa para _crear_ el RDD inicial (`sc.parallelize(subscriptions)`), no para transformarlo.

**Paso 6 (Filtrar posts vacíos):**
No encaja estrictamente en `map`, `flatMap` ni `reduceByKey`. Es un **`filter`**, que es una abstracción propia de Spark. `filter` no transforma elementos ni los reduce: simplemente descarta los que no cumplen un predicado. No produce exactamente un resultado por entrada (puede producir 0 o 1), pero a diferencia de `flatMap`, no cambia el tipo de los elementos.

**Paso 7 (Cargar diccionarios):**
No encaja porque no opera sobre datos distribuidos. El diccionario es un dato de referencia pequeño que se carga en el driver y se envía a todos los workers como **broadcast variable**. Es un patrón común en Spark: datos auxiliares que todos los workers necesitan pero que no vale la pena distribuir como RDD.

**Paso 10 (Ranking top-K):**
No encaja porque es una **acción**, no una transformación. Las acciones de Spark (`collect`, `take`, `takeOrdered`, `count`) provocan la ejecución del DAG y traen los resultados al driver. Ordenar globalmente y tomar los top-K requiere visión completa de los datos, lo cual es inherentemente no paralelizable como transformación.

---

### c_ Barreras de sincronización vs. pasos independientes

#### Clasificación de cada paso

__________________________________________________________________________________________
| Paso |        Descripción        |      Clasificación       |       Justificación       |
|------|---------------------------|--------------------------|---------------------------|
|  1   | Leer suscripciones        |**Secuencial (Driver)**   | Se ejecuta en el driver   |
|      |                           |                          |antes de crear el RDD. No  |
|      |                           |                          |involucra workers.         |
|------|---------------------------|--------------------------|---------------------------|
|  2   | Filtrar suscripciones     |**Secuencial (Driver)**   | Igual que el paso 1: es   |
|      | válidas                   |                          |código del driver, previo  |
|      |                           |                          |a la distribución.         |
|------|---------------------------|--------------------------|---------------------------|
|  3   | Descargar feeds (map)     |**Independiente**         | Cada worker descarga su   |
|      |                           |                          |URL sin necesitar datos de |
|      |                           |                          |otros workers. No hay      |
|      |                           |                          |comunicación entre         |
|      |                           |                          |particiones.               |
|------|---------------------------|--------------------------|---------------------------|
|  4   | Parsear JSON → Posts      |**Independiente**         | Cada worker parsea su     |
|      | (flatMap)                 |                          |propio JSON localmente. No |
|      |                           |                          |depende de resultados de   |
|      |                           |                          |otros workers.             |
|------|---------------------------|--------------------------|---------------------------|
|  5   | Aplanar posts             |**Independiente**         | Implícito en el flatMap   |
|      | (implícito en flatMap)    |                          |del paso 4. Cada partición |
|      |                           |                          |aplana sus propios         |
|      |                           |                          |resultados localmente.     |
|------|---------------------------|--------------------------|---------------------------|
|  6   | Filtrar posts vacíos      |**Independiente**         | Cada worker aplica el     |
|      | (filter)                  |                          |predicado booleano solo a  |
|      |                           |                          |sus posts locales. No      |
|      |                           |                          |necesita datos de otros    |
|      |                           |                          |workers.                   |
|------|---------------------------|--------------------------|---------------------------|
|  7   | Cargar diccionarios       |**Secuencial (Driver) +** | El driver carga los       |
|      | (broadcast)               |**broadcast**             |diccionarios y los envía a |
|      |                           |                          |todos los workers. Es un   |
|      |                           |                          |punto de sincronización    |
|      |                           |                          |implícito: el broadcast    |
|      |                           |                          |debe completarse antes de  |
|      |                           |                          |que los workers usen el    |
|      |                           |                          |diccionario en el paso 8.  |
|------|---------------------------|--------------------------|---------------------------|
|  8   | Detectar entidades        |**Independiente**         | Cada worker procesa sus   |
|      | (flatMap)                 |                          |posts contra la broadcast  |
|      |                           |                          |variable local. No necesita|
|      |                           |                          |datos de otros workers.    |
|------|---------------------------|--------------------------|---------------------------|
|  9   | Contar entidades          |**🚧 BARRERA 🚧**        | `reduceByKey` implica un  |
|      | (map + reduceByKey)       |                          |**shuffle**: los datos se  |
|      |                           |                          |redistribuyen por clave    |
|      |                           |                          |entre todos los workers.   |
|      |                           |                          |Ningún worker puede        |
|      |                           |                          |producir el conteo final de|
|      |                           |                          |una entidad hasta que TODOS|
|      |                           |                          |los workers hayan terminado|
|      |                           |                          |de emitir sus pares        |
|      |                           |                          |clave-valor. Es una        |
|      |                           |                          |**barrera de               |
|      |                           |                          |sincronización**.          |
|------|---------------------------|--------------------------|---------------------------|
|  10  | Ranking top-K             |**🚧 BARRERA 🚧**        | `collect`/`takeOrdered`   |
|      | (acción)                  |                          |es una **acción** que      |
|      |                           |                          |requiere que TODOS los     |
|      |                           |                          |workers terminen el paso 9 |
|      |                           |                          |y envíen sus resultados    |
|      |                           |                          |al driver. Ningún worker   |
|      |                           |                          |puede producir el ranking  |
|      |                           |                          |parcial sin visión global. |
|______|___________________________|__________________________|___________________________|

---

#### Resumen visual

```
  Driver (secuencial)         Workers (paralelo)         Barreras
  ═══════════════════         ══════════════════         ════════
  ┌─────────────────┐
  │ 1. Leer subs    │
  │ 2. Filtrar subs │
  └────────┬────────┘
           │ sc.parallelize(...)
           ▼
                        ┌────────────────────────┐
                        │ 3. Descargar (map)     │──── Independiente
                        │ 4. Parsear (flatMap)   │──── Independiente
                        │ 5. Aplanar (implícito) │──── Independiente
                        │ 6. Filtrar (filter)    │──── Independiente
                        └────────────────────────┘
  ┌─────────────────┐            │
  │ 7. Cargar dict  │────broadcast──→ todos los workers
  └─────────────────┘            │
                        ┌────────────────────────┐
                        │ 8. Detectar entidades  │──── Independiente
                        │    (flatMap)           │
                        └────────┬───────────────┘
                                 │
                        ╔════════╧════════════════╗
                        ║ 9. Contar entidades     ║──── 🚧 BARRERA (shuffle)
                        ║    (reduceByKey)        ║      Todos los workers deben
                        ╚════════╤════════════════╝     emitir antes de reducir
                                 │
                        ╔════════╧════════════════╗
                        ║ 10. Ranking top-K       ║──── 🚧 BARRERA (acción)
                        ║    (collect/takeOrdered)║      Todos deben terminar
                        ╚═════════════════════════╝     antes de recolectar
```

---

#### ¿Por qué las barreras son necesarias?

**Paso 9 (reduceByKey) es barrera** porque para saber cuántas veces aparece, por ejemplo, "Scala" en total, es necesario que **todos** los workers hayan terminado de procesar **todos** sus posts. Si el worker 1 encontró "Scala" 5 veces y el worker 2 encontró "Scala" 3 veces, el conteo final (8) solo puede calcularse después de que ambos terminen. Internamente, Spark ejecuta un **shuffle**: redistribuye los pares `((tipo, nombre), 1)` por clave a través de la red, de modo que todos los valores de la misma clave terminen en el mismo nodo para ser sumados. Este shuffle es la barrera.

**Paso 10 (collect/takeOrdered) es barrera** porque para determinar los top-K a nivel global, se necesita el resultado completo del paso 9. Aunque las reducciones parciales ya ocurrieron, el driver necesita recolectar todos los conteos finales de todas las particiones para ordenarlos globalmente. Es una barrera porque ningún resultado parcial de un worker individual constituye la respuesta final.

#### ¿Por qué los demás pasos NO son barreras?

Los pasos 3, 4, 5, 6 y 8 son **transformaciones narrow** (no requieren shuffle). Cada worker opera exclusivamente sobre los datos de su propia partición:

- **map** (paso 3): un elemento de entrada → un elemento de salida, sin comunicación.
- **flatMap** (pasos 4, 5, 8): un elemento → N elementos, todos locales al worker.
- **filter** (paso 6): mantiene o descarta localmente, sin ver datos de otros.

Ninguno de estos pasos necesita que otro worker termine para poder proceder. Los workers ejecutan estos pasos en paralelo, cada uno a su propio ritmo, sin esperarse mutuamente.

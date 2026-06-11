# Explicación paso a paso de los incisos 1 a, b y c

## 1. Conexión — **Driver puro (No encaja en transformaciones)**
**Razonamiento:** Para que los workers (los obreros del cluster) puedan arrancar, tu computadora central (Driver) debe primero decodificar la misión inicial. La lectura del "subscriptions.json" es local a tu disco duro. 
- ¿Esto mapea a `map` o `flatMap`? No, porque en este punto ni siquiera existe la abstracción Spark del RDD ni fluyen datos por red. La "conexión" teórica de los inputs corresponde al lapso en que el Driver importa los hilos de suscripción y formaliza y distribuye los chunks iniciales vía `sc.parallelize()`.
- **¿Es independiente o barrera?** Técnicamente ninguna dentro de la lógica distribuida. Resulta ser estrictamente Secuencial. Ocurre enteramente bajo la ejecución del Master antes de arrancar los otros nodos.

## 2. Descarga — **flatMap (Transformación Narrow / Independiente)**
**Razonamiento:** Spark arranca la paralelización y reparte el lote de recursos. A un worker aleatorio le cae "reddit.com/r/scala". 
- Ejecuta abrir el feed de Reddit. El json devuelto trae la matriz de diccionarios `data.children` conformada de **múltiples** posts.
- **¿Por qué flatMap?** Porque una función toma como Input únicamente "1 enlace" y despliega hacia Output "N objetos Post". Ese ratio de 1:N es la finalidad fundamental que solventa un `flatMap` en Spark, eliminando las horribles "listas de listas" de tu pipeline resultante.
- **¿Por qué es Independiente?** Que el Worker A tarde conectándose y bajando el listado del subreddit de Scala no ejerce dependencia ninguna en absoluto sobre el Worker B bajando datos de C++. Trabajan a su marcha de forma atómica.

## 3. Extracción de entidades — **flatMap (Transformación Narrow / Independiente)**
**Razonamiento:** Ahora tu worker tiene en su memoria volátil local su porción limpia de posts descargados (`List[Post]` interna de su RDD).
- Aplica el mapeo lógico utilizando la variable propagada Broadcast que almacena todos los diccionarios pre-cargados al inicio, limpiando signos e individualizando palabras.
- **¿Por qué flatMap?** Misma situación. Analizar la cadena de texto de un (1) sólo Post, produce cero (0), una (1) o muchísimas (N) apariciones concretas del objeto `NamedEntity`. El despliegue de 1 a N es flatMap de manual.
- **¿Por qué es Independiente?** Analizar un texto asilado para reconocer la palabra "Oracle" no estanca bloqueos, no se fijará nunca en los hallazgos de otras máquinas. Transformación lineal y estrecha (Narrow Dependency).

## 4. Clasificación — **map (Transformación Narrow / Independiente)**
**Razonamiento:** El framework precisa saber a final de cuentas de quién es el texto para agruparlo después y llevar su métrica de agregación. 
- A cada Worker se le indica que acomode a clave unívoca y "uno por default" el conteo. Tomas tu `NamedEntity` y reasignas su valor final como la tupla agrupable clave-valor matemática: `(("Organization", "Oracle"), 1)`.
- **¿Por qué map?** Una entidad ya extraida pasa por el filtro devolviendo irreductiblemente una y solo una tupla asignada (ratio estricto 1:1). 
- **¿Por qué es Independiente?** Embolsar el valor es un cálculo directo y unitario.

## 5. Conteo — **reduceByKey (Wide Transformation / 🚧 BARRERA 🚧)**
**Razonamiento:** En este punto el master mira la nube. Hay RDDs con valores `"Python", 1` desperdigados y cacheados a la vez en el Worker 1, Worker 5, y Worker 8. Queremos totalizar contadores a lo largo de un listado final.
- **¿Por qué reduceByKey?** Mapea indiscutiblemente bien. Es la navaja suiza de Spark. Agrupará y proveerá una iteración interna de reducción matemática clásica donde dos o más tuplas de misma llave se aplastan sumando `1 + 1`.
- **¿Por qué es BARRERA DE SINCRONIZACIÓN?** ¡El núcleo central del aprendizaje paralelo! Un Worker individual de tu cluster no tiene ni la potestad ni los datos para avisar "Misión lista, Python detectado 200 veces". ¿Por qué no? Porque el dato está **aislado**. Spark activará obligatoriamente la ejecución de red más demandante en Big Data: el **Shuffle**. Interrumpirá a todos, los frenará sin avanzar iteraciones al forzarlos a mover y volcar por la red interna sus tablas extraídas entre sí a fin de que todas las apariciones mundiales de "Python" caigan para su reducción sumada en el *mismo worker*. Ninguno está permitido a continuar hacia emitir el listado al Driver hasta que se hayan reportado la totalidad del cruce y suma a destajo general.

## 6. Ranking — **Acción Terminal de Driver (🚧 BARRERA 🚧)**
**Razonamiento:** Spark logró un RDD consolidado e íntegro bajo tu poder distribuido; pero la vista a terminal del RDD no es real. 
- **¿Abstracción utilizada?** No es un método `reduceByKey`, estas son herramientas *lazy-evaluated* (posponen cálculo y prometen devolver otra porción del RDD iterativamente virtual). Este punto 6 le urge una consolidación terrenal, requiere las funciones `take(N)`, `takeOrdered(K)` o el temible `collect()`. Esto se denomina **Acción**.
- **¿Por qué es barrera?** Desata de un golpe el pedido completo hacia todas las filas previas que vinimos posponiendo, rompe la asincronía y fuerza al Driver a esperar como sumidero único sin finalización posible del ciclo principal de ejecución hasta arrastrar, ordenar masivamente todos en sus rutinas y arrojar el dictamen jerárquico a tu pantalla estandarizada en formato Markdown.

---

### Resumen visual lógico y asincrónico

```text
  Driver (secuencial)         Workers (paralelo)         Barreras
  ═══════════════════         ══════════════════         ════════
  ┌─────────────────┐
  │ 1. Conexión     │
  └────────┬────────┘
           │ sc.parallelize(s) 
           ▼
                        ┌─────────────────────────┐
                        │ 2. Descarga (flatMap)   │──── Independiente
                        │ 3. Extracción (flatMap) │──── Independiente
                        │ 4. Clasificación (map)  │──── Independiente
                        └────────┬────────────────┘
                                 │
                        ╔════════╧════════════════╗
                        ║ 5. Conteo (reduceByKey) ║──── BARRERA (shuffle)
                        ╚════════╤════════════════╝  
                                 │
  ┌─────────────────┐            │
  │ 6. Ranking (top)│◄───────────┘                      BARRERA (Acción final)
  └─────────────────┘ 
```

---

# Explicación paso a paso — Ejercicio 1d

## Cómo llegué a la conclusión sobre las restricciones de Spark

### El planteo del problema
El problema pregunta qué restricciones impone Spark sobre las funciones proporcionadas por el usuario (los closures que se pasan a `map`, `filter`, `reduceByKey`, etc.) considerando que se ejecutarán en un entorno distribuido, haciendo énfasis en tres áreas teóricas fundamentales: **serialización**, **estado compartido** y **efectos secundarios**.

Para responder esto la premisa a adoptar en tu cabeza es: **Dejar de pensar como si ejecutaras el proceso de la lista en un for-loop en tu computadora**, y abstraerse en el **modelo de ejecución distribuida**.

### 1. Serialización
**Qué deberías entender teóricamente:** La diferencia y separación de espacio entre Driver y Workers y cómo se transporta la carga de un lugar a otro.
El código de la aplicación se inicializa en un proceso principal ("Driver", ej. el script central enviando desde tu PC). Pero los datos a procesar (RDD) están repartidos en varios nodos ("Workers"). Cuando escribes en tu driver `rdd.map(x => x + miVariableExterna)`, Spark necesita aislar ese bloque de código `x => x + miVariableExterna`, empaquetarlo, mandarlo por la red (TCP) para que los workers hagan el trabajo con los datos locales a ellos.
**Cómo resolverlo:** ¿Cómo viaja un objeto instanciado en Java por la red hacia otra memoria y reconstruye su forma? Por medio de la **Serialización** (proceso de transformar un objeto a bits transportables). Entonces, la primera restricción recae en que **todo el código (y por consecuencia natural cada variable u objeto externo que "atrape" tu bloque lógico) obligatoriamente debe ser Serializable**. Si ese closure atrapa algo como una conexión HTTP, variables no inicializadas correctamente, o conexiones JDBC, Spark te castigará tirando un clásico `NotSerializableException` antes de arrancar.

### 2. Estado Compartido (Shared State)
**Qué deberías entender teóricamente:** El aislamiento y asincronía de memoria RAM.
Cuando usas variables locales e integras un contador como `var counter = 0; rdd.foreach(x => { counter += 1; ... })`, Spark debe serializar el contador local e introducir eso al empaque del closure. Lo envía. 
**Cómo resolverlo:** Cada worker, en su RAM apartada e independiente del cluster, recibe y tiene ahora **su propia y apartada copia** de la memoria local, no es el mismo puntero al objeto raíz original (no hay apuntadores compartidos a distancia). Cuando un worker incremente el contador, actualizará en 1 "su versión". Ningún otro cluster ni el creador (el driver principal) conocerá de esta acción y `counter` seguirá arrojando valiendo `0` en la fase final; para Spark compartir mutuamente y retornar estas combinaciones se hace vía *Variables Acumuladores*. Por tanto la conclusión es **ausencia total de estado compartido mutado por el usuario**, no se mutan variables globales si la intención de esas variables depende de que la operación las sume distribuidamente, pues la sumará localmente.

### 3. Efectos Secundarios (Side Effects)
**Qué deberías entender teóricamente:** La cualidad de Resiliencia a las fallas (Tolerenacia a Fallas y Linaje).
El framework existe para la escalabilidad: asume por fondo que con cientos de computadores, alguna PC o conexión siempre va a fallar antes de tener el proceso terminado. Si el Worker 3 se quema por la mitad de un `map`, Spark no tumba toda tu Big Data. Retoma de su Lineage (su plan de pasos guardado en memoria) y ordena repetir y regenerar a Worker 4 lo que Worker 3 iba logrando. Más allá del daño, cuenta con que si Worker 1 avanza lento, enciende un proceso idéntico especulativo a procesar junto a la par a ver cuál llega más rápido y descartará un avance idéntico al otro. 
**Cómo resolverlo:** Asumiendo que tu función base, ej: `map(x => notificarUsuarioPorMail(x))`, en casos normales de hardware dañado un fragmento de tu mapeo fue procesado 1,5 o incluso tres veces por un nodo, se enviaron tres correos repitiendo y duplicando los problemas para tus clientes. Ante ello, la restricción final a concluir es que las extensiones deben abstenerse estrictamente a ser **Puramente Idempotentes** en acciones a ser tomadas sin sufrir cambios (es decir una función "pura" donde procesar una o tres veces conllevan el mismo resultado en vez del crecimiento constante). No deberías causar efectos desde transformaciones, sólo al final, en bloques dedicados de salida cuidando atomicidades.

---

# Explicación paso a paso — Ejercicio 5b: Optimización con Persistencia

## Identificación de Re-cómputos
Spark es *lazy* por naturaleza. Esto significa que cuando definimos un RDD (como `downloadResults`), Spark no hace nada hasta que llamamos a una **Acción** (como `.count()` o `.collect()`). 

El problema surge cuando llamamos a múltiples acciones sobre el mismo RDD. Sin persistencia, Spark vuelve a ejecutar todo el "linaje" (la cadena de pasos) desde el principio para cada acción. En nuestro caso, esto es críticamente ineficiente porque:
1. **`subscriptions`**: Se usa 3 veces para contar éxitos y fallos. Aunque es un RDD pequeño, re-leerlo o re-paralelizarlo es trabajo extra innecesario.
2. **`downloadResults`**: ¡ESTE ES EL PUNTO CRÍTICO! Este RDD involucra llamadas de red (`downloadFeed`). Se usa en 5 puntos distintos. Sin `.cache()`, ¡estaríamos bajando los mismos feeds de Reddit 5 veces seguidas! 
3. **`filteredPosts`**: Se usa 5 veces (para conteos, cálculo de promedio de caracteres, verificar si está vacío y finalmente extraer entidades). Cada vez que lo usamos, si no estuviera cacheado, Spark tendría que filtrar de nuevo los resultados descargados.

## Solución aplicada: Uso de `.cache()`
He aplicado `.cache()` inmediatamente después de la definición de estos tres RDDs:

- **`subscriptions.cache()`**: Asegura que la lista de suscripciones parseada se mantenga en memoria para los distintos conteos iniciales.
- **`downloadResults.cache()`**: Fundamental. La primera vez que se ejecute una acción (ej: `downloadResults.count()`), Spark descargará los feeds y guardará los objetos `Post` en la memoria RAM de los workers. Las siguientes 4 veces que se use, Spark leerá los posts directamente de la memoria, ahorrando todo el tráfico de red y el parseo JSON.
- **`filteredPosts.cache()`**: Almacena el subconjunto de posts válidos. Dado que se atraviesa varias veces para estadísticas y procesamiento NER, tenerlo ya filtrado y listo en memoria acelera drásticamente la etapa final del pipeline.

**Nota conceptual:** `.cache()` es equivalente a `.persist(StorageLevel.MEMORY_ONLY)`. Como estamos trabajando en modo local con datasets que entran cómodamente en la RAM, esta es la opción más rápida.

---

# Explicación paso a paso — Ejercicio 5c: Gestión de Memoria con unpersist()

## Liberación Progresiva de Recursos
Tan importante como persistir los datos es liberarlos cuando ya no son necesarios, especialmente en entornos con recursos limitados. El método `.unpersist()` le indica a Spark que puede eliminar los datos de la memoria RAM de los workers.

He implementado la liberación de memoria en los siguientes puntos estratégicos:

1.  **`subscriptions.unpersist()`**: 
    *   **Cuándo**: Tan pronto como terminamos de contar los posts descargados (`postsFailed`).
    *   **Por qué**: En este punto del programa, ya tenemos los posts en `downloadResults` (que está cacheado). Ya no necesitamos volver a las URLs originales de las suscripciones.

2.  **`filteredPosts.unpersist()`**:
    *   **Cuándo**: Inmediatamente después de llamar a `.collect()` para obtener la lista de entidades en el driver (`allEntitiesRDD`).
    *   **Por qué**: Una vez que los datos han sido procesados y traídos al driver como objetos locales de Scala, el RDD en el cluster ya no cumple ninguna función.

3.  **`downloadResults.unpersist()`**:
    *   **Cuándo**: Al final de todo el procesamiento, después de la última acción del Ejercicio 3.
    *   **Por qué**: Se mantiene hasta el final porque el segundo bloque de análisis de entidades (implementado enteramente con RDDs) depende de él.

## Manejo de Casos de Error
El requerimiento 5c hace énfasis en los casos de error. He añadido llamadas a `.unpersist()` en todos los puntos de salida prematura (`return`):
- Si no hay suscripciones válidas, liberamos `subscriptions`.
- Si no se bajaron posts válidos o si el directorio de entidades no existe, liberamos tanto `downloadResults` como `filteredPosts`.

Esto asegura que, independientemente de si el programa termina con éxito o por un error, no dejamos "basura" ocupando memoria en el cluster/sesión de Spark.

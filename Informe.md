# Informe — Laboratorio 3

## Ejercicio 1: Identificar las regiones paralelizables

### a_ Grafo de dependencias

**Paso 1 — Conexión**
  El Driver lee las suscripciones desde el origen de datos/archivo local y distribuye los registros para iniciar el clúster.

**Paso 2 — Descarga**
  Cada Worker toma sus asignaciones, abre la conexión a la URL externa (HTTP), descarga el contenido y decodifica localmente su propio documento JSON, aplanando los datos útiles para producir una secuencia de Posts.

**Paso 3 — Extracción de entidades**
  Cada Worker recorre el texto de sus Posts locales, consultando un diccionario auxiliar distribuido previamente ("broadcast"), en búsqueda de coincidencias limpiadas para arrojar una lista de menciones de Entidades.

**Paso 4 — Clasificación**
  Cada Worker reempaqueta cada mención de entidad asignándole un valor base de ocurrencia matemática, dejándola estandarizada mediante una clave primaria (`TipoDeEntidad, NombreDeLaEntidad`) lista para la fase de reducción.

**Paso 5 — Conteo**
  Los Workers entablan comunicación cruzada (Shuffle) por la red local del cluster para reagrupar todas las coincidencias atadas por la misma clave y reducir (sumar matemáticamente) sus contadores iterativamente hasta que el cluster concuerde los conteos unificados de cada entidad.

**Paso 6 — Ranking**
  El Driver envía una orden terminal a los workers: se trae (recolecta) los contadores consolidados devueltos del Paso 5 e interrumpe el paralelismo, ordenándolos él mismo en su hilo principal de forma global para extraer las más repetidas a pantalla.

____________________________________________________________________________________
| Arista |          Output de → Input de           |           Tipo Scala          |
|--------|-----------------------------------------|-------------------------------|
| 1 → 2  | Conexión → Descarga                     | `RDD[Subscription]`           |
|--------|-----------------------------------------|-------------------------------|
| 2 → 3  | Descarga → Extracción de entidades      | `RDD[Post]`                   |
|--------|-----------------------------------------|-------------------------------|
| 3 → 4  | Extracción de ents. → Clasificación     | `RDD[NamedEntity]`            |
|--------|-----------------------------------------|-------------------------------|
| 4 → 5  | Clasificación → Conteo                  | `RDD[((String, String), Int)]`|
|--------|-----------------------------------------|-------------------------------|
| 5 → 6  | Conteo → Ranking                        | `RDD[((String, String), Int)]`|
|________|_________________________________________|_______________________________|

---

### b_ Mapeo de cada paso a abstracciones de Spark

______________________________________________________________________________________
| Paso |    Descripción       |   Abstracción Spark    |        Justificación        |
|------|----------------------|------------------------|-----------------------------|
|  1   | Conexión             | **No encaja** (Driver) | Ocurre en el Driver antes   |
|      |                      |                        | de las transformaciones     |
|      |                      |                        | en el cluster,              |
|      |                      |                        | materializa el RDD inicial. |
|------|----------------------|------------------------|-----------------------------|
|  2   | Descarga             | **flatMap**            | Partiendo de un elemento    |
|      |                      |                        | (`Subscription`), baja JSON |
|      |                      |                        | y produce 0 o más `Post`.   |
|------|----------------------|------------------------|-----------------------------|
|  3   | Extracción de Ents.  | **flatMap**            | Por cada Post evaluado,     |
|      |                      |                        | produce N `NamedEntity`     |
|      |                      |                        | usando diccionarios.        |
|------|----------------------|------------------------|-----------------------------|
|  4   | Clasificación        | **map**                | Transforma cada elemento en |
|      |                      |                        | exactamente 1 resultado: la |
|      |                      |                        | tupla `((Tipo, Nombre), 1)` |
|------|----------------------|------------------------|-----------------------------|
|  5   | Conteo               | **reduceByKey**        | Combina múltiples valores   |
|      |                      |                        | asociados a misma clave     |
|      |                      |                        | a través del clúster.       |
|------|----------------------|------------------------|-----------------------------|
|  6   | Ranking              | **No encaja** (Driver) | Es una **Acción** terminal  |
|      |                      |                        | como `collect`/`takeOrdered`|
|      |                      |                        | para consolidar resultados. |
|______|______________________|________________________|_____________________________|

---

### Pasos que NO encajan en map/flatMap/reduceByKey

**Paso 1 (Conexión):**
No encaja en las transformaciones funcionales distribuidas indicadas ya que ocurre pura y exclusivamente del lado del nodo Principal (driver), que levanta un conjunto de datos estáticos desde E/S inicial externa y luego distribuye la tarea subiéndola a un RDD inicial para los Trabajadores (`sc.parallelize()`).

**Paso 6 (Ranking):**
Mientras las transformaciones en el interior del marco del problema son _procastinadas (lazy evaluadas)_, este paso encierra una **acción terminal** (`collect`, `takeOrdered`, etc.). Rompe la inercia lógica forzando a los workers a reportar el cómputo final consolidado hacia la memoria del driver o un puerto de base de datos final, finalizando la fase distribuida.

---

### c_ Barreras de sincronización vs. pasos independientes

#### Clasificación de cada paso

__________________________________________________________________________________________
| Paso |        Descripción        |      Clasificación       |       Justificación       |
|------|---------------------------|--------------------------|---------------------------|
|  1   | Conexión                  |**Secuencial (Driver)**   | Proviene del master al    |
|      |                           |                          | iniciar. Fase sin workers.|
|------|---------------------------|--------------------------|---------------------------|
|  2   | Descarga (flatMap)        |**Independiente**         | Worker realiza petición   |
|      |                           |                          | independiente. Trans.     |
|      |                           |                          | "Narrow".                 |
|------|---------------------------|--------------------------|---------------------------|
|  3   | Extracción (flatMap)      |**Independiente**         | Aplica lógica funcional a |
|      |                           |                          | texto aislado.            |
|------|---------------------------|--------------------------|---------------------------|
|  4   | Clasificación (map)       |**Independiente**         | Asignación sintáctica     |
|      |                           |                          | clave-valor sin estado    |
|      |                           |                          | global cruzado.           |
|------|---------------------------|--------------------------|---------------------------|
|  5   | Conteo (reduceByKey)      |**BARRERA**               | Requiere redistribución   |
|      |                           |                          | obligatoria (Shuffle por  |
|      |                           |                          | red TCP) con otros nodos. |
|------|---------------------------|--------------------------|---------------------------|
|  6   | Ranking (Acción)          |**BARRERA**               | Ordena esperar todo el    |
|      |                           |                          | final de reducciones en   |
|      |                           |                          | el sumidero Driver final. |
|______|___________________________|__________________________|___________________________|

#### ¿Por qué las barreras son necesarias?

**El Paso de Conteo (Paso 5) es una barrera:** Spark invoca para la agrupación y suma una **Wide Dependency**; no se puede declarar contada en total a la entidad "Scala = 5 veces" usando un solo worker parcial, pues esa entidad bien podría ser el output disperso en N workers en otros servidores. Para sumarlos y contar, este paso bloqueante inicia **un Shuffle** por red, enviando todos los registros de la misma clave esparcidos dispersos para que aterricen en el mismo worker reductor que se encargará materialmente de sumarlo. El reductor no puede enviar respuesta sin estar totalmente seguro que todos terminaron de emitir sus extracciones.

**El Paso de Ranking (Paso 6) es una barrera:** Como driver que dictamina fin de pipeline, recoger absolutos unificados mediante `collect` u ordenarlos globalmente de todo el cluster es una detención del ciclo paralelo para un retorno sincrónico y secuencial con el script en consola.

#### ¿Por qué los demás pasos NO son barreras?

Los pasos de **Descarga (2)**, **Extracción (3)**, y **Clasificación (4)** conforman el grupo de transformaciones **Narrow** (estrechas). Su regla principal asegura que: cada partición producida de cálculos derivan únicamente del input de la misma partición y nada más. Cada Worker transcurre operando su listado local a su ritmo individual libre de esperas sobre los estados de las particiones del resto.

---

### d_ Restricciones sobre las funciones de extensión en Spark

En Spark, el mecanismo principal de extensión son las funciones que el desarrollador provee a operaciones como `map`, `flatMap`, `filter` y `reduceByKey`. Dado que estas funciones se definen en el programa principal pero se ejecutan en los nodos de cómputo (workers) de un entorno distribuido, Spark impone o asume ciertas restricciones críticas sobre ellas para garantizar un correcto funcionamiento:

1. **Serialización**:
   - **Restricción**: Toda la función (como closure) y cualquier variable de su entorno léxico que referencie internamente, debe ser serializable.
   - **Justificación**: El programa (el driver) compila y define el closure. Sin embargo, para ejecutarlo en un worker remoto, Spark debe convertir ese bloque de memoria en una secuencia de bytes, viajar por la red mediante RPC, y materializar la función en la Máquina Virtual de Java del worker. Si la función depende de un objeto que no puede serializarse (por ejemplo, una conexión a base de datos persistente o un socket), Spark no puede enviarla, lo que resulta en un error de compilación o runtime (`NotSerializableException`).

2. **Ausencia de estado mutado compartido (Shared State)**:
   - **Restricción**: Las funciones no deben depender de la posibilidad de modificar directamente un estado global compartido.
   - **Justificación**: Los workers operan en un espacio de memoria aislado (RAM física separada). Si tu función hace referencia a una variable local definida en el driver y la modifica (ej: incrementa un contador global `counter += 1`), las variaciones solo sucederán sobre una **copia local** de esa variable transferida a ese worker particular. Al driver original no le llegará esta alteración ni la compartirán entre distintos workers, manteniéndose el valor base en `0`. La comunicación de información y agregados en Spark depende siempre de los mecanismos puros (`reduce`, `groupBy`), o de herramientas especiales diseñadas para la distribución como *Broadcast Variables* (estado inmutable y de solo lectura común a todos, utilizado en el paso 7 de nuestro lab) y *Accumulators* (varibles de solo-escritura).

3. **Sin efectos secundarios (Side Effects) o limitados a idempotencia**:
   - **Restricción**: Las funciones pasadas a las transformaciones deberían tender a ser determinísticas e idempotentes (funciones "puras"), o si causan mutaciones externas (bases de datos, archivos), estas no deberían depender del número de veces que se las ejecute.
   - **Justificación**: Spark provee una robusta tolerancia a fallas. Cuando un worker falla o la red se desconecta antes de terminar su sección de procesamiento de datos, o incluso si Spark nota que un worker opera más lento que los demás (stragglers), Spark puede y va a **ejecutar de nuevo, reactivar o duplicar (especular)** ese trabajo fallido en otro worker. Ante ello, no hay garantía estricta de que tu transformación sobre un dato ejecute una única vez. Si una etapa produce resultados transaccionales, envía correos o debita dinero, un fallo provocaría que el correo se envíe en repetidas ocasiones en la repetición del bloque. Todo el trabajo final que interactúe puramente hacia el exterior se suele posponer a las Acciones de tipo volcado (`foreachPartition`) donde se emplean sentencias manejando la repetición y las transacciones idempotentemente y atómicamente.

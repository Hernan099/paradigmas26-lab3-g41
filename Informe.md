# Informe — Laboratorio 3

## Ejercicio 1: Identificar las regiones paralelizables

### Detalle de cada conexión (arista)

| Arista |                            Output de → Input de                             |           Tipo Scala          |
|--------|-----------------------------------------------------------------------------|-------------------------------|
| 1 → 2  | Suscripciones leídas del JSON (pueden ser `None` si malformadas)            | `List[Option[Subscription]]`  |
| 2 → 3  | Solo las suscripciones válidas (se descartaron los `None`)                  | `List[Subscription]`          |
| 3 → 4  | Resultados de descarga: tupla (éxito?, posts parseados)                     | `List[(Boolean, List[Post])]` |
| 4 → 5  | Mismo tipo; la descarga y el parseo ocurren dentro del mismo `map`          | `List[(Boolean, List[Post])]` |
| 5 → 6  | Todos los posts de todos los feeds, aplanados en una sola lista             | `List[Post]`                  |
| 6 → 8  | Posts no vacíos, listos para análisis                                       | `List[Post]`                  |
| 7 → 8  | Diccionario completo de entidades conocidas (cargado desde archivos `.txt`) | `List[NamedEntity]`           |
| 8 → 9  | Todas las entidades detectadas en todos los posts                           | `List[NamedEntity]`           |
| 9 → 10 | Conteo de cada entidad agrupada por `(entityType, entityName)`              | `Map[(String, String), Int]`  |

---


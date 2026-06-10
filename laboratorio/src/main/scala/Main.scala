import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import java.io.File
object Main {
  def main(args: Array[String]): Unit = {
    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }
    //creamos una Sparck sesion de forma local
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()


    val sc = spark.sparkContext

    // ERROR HANDLING: Load subscriptions con try-catch para manejo de errores de archivo
    val subscriptionOpts = try {
      sc.parallelize(FileIO.readSubscriptions(cmdArgs.subscriptionFile))
    } catch {
      case _: java.io.FileNotFoundException =>
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} - file not found")
        return
      case _: Exception =>
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} - invalid JSON format")
        return
    }

    // Filter out malformed subscriptions (None values)
    val feedAcum = sc.longAccumulator("succesful feed")
    val failFeedAcum = sc.longAccumulator("failed feed")
    
    val subscriptions: RDD[Subscription] = subscriptionOpts.flatMap {
      case Some(s) =>
        if (s.name.isEmpty || s.url.isEmpty){ 
          failFeedAcum.add(1)
          println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
          Iterator.empty
        }
        else {
          feedAcum.add(1)
          Iterator(s)
        }
      case None => Iterator.empty
    }

    // Download feeds and parse posts, tracking success/failure , como lo estamos paralelizando con flatmap, lo que hacemos es:
    //para cada subscripcion, la descargamos, luego, si esta no tiene nada, ponemos iterador.empty, si si tiene algo, sumamos
    //el contenido parceado
    val postAcum = sc.longAccumulator("succesful posts")
    val failPostAcum = sc.longAccumulator("failed feed")
    // REQUISITO B: flatMap que descarga feeds y devuelve TODOS los posts (no solo el primero)
    val downloadResults = subscriptions.flatMap { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      
      feedOpt match {
        case None =>
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          failPostAcum.add(1)
          Iterator.empty
        case Some(feedContent) =>
          val posts = JsonParser.parsePosts(feedContent, subscription.name)
          if (posts.isEmpty){
            failPostAcum.add(1)
            Iterator.empty
          } else {
            postAcum.add(1)
            posts.iterator
          }
      }
    }

    // Count feed successes/failures lo hacemos con lo que tenemos
    val feedsSuccess = subscriptions.count()
    println(feedAcum.value)
    val feedsFailed = subscriptionOpts.count() - subscriptions.count()
    println(failFeedAcum.value)

    // ERROR HANDLING: Validar que hay suscripciones válidas
    if (feedsSuccess == 0) {
      println("Error: No valid subscriptions found")
      return
    }
    // Flatten all posts and count JSON parse failures
    //borramos una variable que guardaba un map con otodos los post, que ya o nescesitamos, y cambiamos todo para poder sacar lo que necesita
    val postsSuccess = downloadResults.count() 
    println(postAcum.value)
    val postsFailed = subscriptions.count() - downloadResults.count()
    println(failPostAcum.value)
    
    // Filter empty posts
    val filteredPosts: RDD[Post] = downloadResults.filter(post => post.title.nonEmpty && post.selftext.nonEmpty)
    val postsFiltered = downloadResults.count() - filteredPosts.count()

    // Calculate average characters in filtered posts
    val kept = filteredPosts.count()
    val totalChars: Long = if (kept == 0) 0L
      else filteredPosts.map(post => post.title.length.toLong + post.selftext.length.toLong).reduce(_ + _)
    val avgChars: Double = if (kept > 0) totalChars.toDouble / kept else 0.0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess.toInt,
      "feedsFailed" -> feedsFailed.toInt,
      "postsSuccess" -> postsSuccess.toInt,
      "postsFailed" -> postsFailed.toInt,
      "postsFiltered" -> postsFiltered.toInt,
      "avgChars" -> avgChars.toInt
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    // Load dictionaries
    val entitiesDir = new File(cmdArgs.entitiesDir)
    if (!entitiesDir.exists() || !entitiesDir.isDirectory) {
      println(s"Error: entities directory '${cmdArgs.entitiesDir}' not found")
      return
    }
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }
    val allEntitiesRDD = allEntities.collect().toList

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntitiesRDD)
    val typeStats = Analyzer.countByType(allEntitiesRDD)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
  }
  //EJERCICIO 3

  //Encuentro las entidades y genero un nuevo RDD con las entidades completas, es decir, titulo + texto
  //val entidades = downloadResults.flatMap( post => 
  //  val postCompleto = post.title + " " + post.selftext
  //
  //  //Generamos un RDD[NamedEntity]
  //  Analyzer.downloadResults(postCompleto,dictionary))
  
}

  val entidades = downloadResults.flatMap( post =>{
    val postCompleto = post.title + " " + post.selftext

    //Generamos un RDD[NamedEntity]
    val parsedPost = Analyzer.detectEntities(postCompleto,dictionary)})
  
  //Convertimos cada NamedEntity en un mapeo ((tipo,name) 1) para agrupar y sumar luego
  val mapedPost = entidades.map( post => ((post.entityType, post.text), 1))

  //Sumamos el 1 de ada entidad mapeada agrupando por tipo
  val entityCount = mapedPost.reduceByKey( _ + _)


  // RDD[(tipo,nombre), cantidad]
  val orderEntity = entityCount.sortBy(e => (e._2, e._1._1), false)

  println(Formatters.formatEntityStats(orderEntity))

  //FIN EJERCICIO 3


  
}
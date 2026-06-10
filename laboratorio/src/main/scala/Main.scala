import org.apache.spark.sql.SparkSession
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


    // Load subscriptions usado spark, esto es lo que crea el rdd
    val subscriptionOpts = sc.parallelize(FileIO.readSubscriptions(cmdArgs.subscriptionFile))

    // Filter out malformed subscriptions (None values), como usamos rdd, ahora lo hacemos con flatmap, Iterador es el tipo de dato que usa flatmap para 
    // iterar sobre los objetos de rdd, cuando hacemos iterador.empty lo que decimos es: no metas nada, en la logica de la funcion, si algun campo tiene none, no lo incluyas, else incluilo 
    val subscriptions:RDD[Subscription] = subscriptionOpts.flatMap {
      sub => if (sub.name == None || sub.url == None) {
        Iterator.empty
      }
      else{
        Iterator(sub)
      }
    }

    // Download feeds and parse posts, tracking success/failure , como lo estamos paralelizando con flatmap, lo que hacemos es:
    //para cada subscripcion, la descargamos, luego, si esta no tiene nada, ponemos iterador.empty, si si tiene algo, sumamos
    //el contenido parceado
    val downloadResults = subscriptions.flatMap { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      val post = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name))
      if (post.isEmpty){
        Iterator.empty
      } else {
      Iterator(post(0))}
  }

    // Count feed successes/failures lo hacemos con lo que tenemos
    val feedsSuccess = subscriptions.count()
    val feedsFailed = subscriptionOpts.count() - subscriptions.count()

    // Flatten all posts and count JSON parse failures
    //borramos una variable que guardaba un map con todos los post, que ya no necesitamos, y cambiamos todo para poder sacar lo que necesita
    val postsSuccess = downloadResults.count()
    val postsFailed = subscripcion.count() - downloadResults.count()

    // Filter empty posts
    val filteredPosts = Analyzer.filterEmptyPosts(allPosts)
    val postsFiltered = allPosts.length - filteredPosts.length
    

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (filteredPosts.nonEmpty) totalChars / filteredPosts.length else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess,
      "feedsFailed" -> feedsFailed,
      "postsSuccess" -> postsSuccess,
      "postsFailed" -> postsFailed,
      "postsFiltered" -> postsFiltered,
      "avgChars" -> avgChars
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
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
  }
  //EJERCICIO 3

  //Encuentro las entidades y genero un nuevo RDD con las entidades completas, es decir, titulo + texto
  val entidades = downloadResults.flatMap( post => 
    val postCompleto = post.title + " " + post.selftext

    //Generamos un RDD[NamedEntity]
  val parsedPost = Analyzer.downloadResults(postCompleto,dictionary))
  
  //Convertimos cada NamedEntity en un mapeo ((tipo,name) 1) para agrupar y sumar luego
  val mapedPost = parsedPost.map( post => ((post.entityType, post.text), 1))

  //Sumamos el 1 de ada entidad mapeada agrupando por tipo
  val entityCount = mapedPost.reduceByKey( _ + _)


  // RDD[(tipo,nombre), cantidad]
  println(entityCount.sortBy(e => e._1._1, e._2))
  
  //FIN EJERCICIO 3


  
}
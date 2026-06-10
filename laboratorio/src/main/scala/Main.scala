import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
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
    //aca commit a1233fd cambie la logica de la funcion porque suscription es un option, entonces use cases para que si es some ahga algo y si es none haga otra cosa    
        val feedAcum = sc.longAccumulator("succesful feed")
        val failFeedAcum = sc.longAccumulator("failed feed")
    val subscriptions: RDD[Subscription] = subscriptionOpts.flatMap {
      case Some(s) =>
        if (s.name.isEmpty || s.url.isEmpty){ 
          failFeedAcum.add(1)
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
    val downloadResults = subscriptions.flatMap { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      val post = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name))
      if (post.isEmpty){
        failPostAcum.add(1)
        Iterator.empty
      } else {
        postAcum.add(1)
        Iterator(post(0))
      }
    }

    // Count feed successes/failures lo hacemos con lo que tenemos
    val feedsSuccess = subscriptions.count()
    println(feedAcum.value)
    val feedsFailed = subscriptionOpts.count() - subscriptions.count()
    println(failFeedAcum.value)
    // Flatten all posts and count JSON parse failures
    //borramos una variable que guardaba un map con otodos los post, que ya o nescesitamos, y cambiamos todo para poder sacar lo que necesita
    val postsSuccess = downloadResults.count() 
    println(postAcum.value)
    val postsFailed = subscriptions.count() - downloadResults.count()
    println(failPostAcum.value)
    // Filter empty posts
    val filteredPosts: RDD[Post] = downloadResults.filter(post => post.title.nonEmpty && post.selftext.nonEmpty)
    val postsFiltered = downloadResults.count() - filteredPosts.count()

    // Calculate average characters in filtered postss
    val totalChars = filteredPosts.flatMap(post => 
    Iterator(post.title.length + post.selftext.length))
    val avgChars = if (!filteredPosts.isEmpty()){ totalChars.reduce(_+_) / filteredPosts.count()} else 0

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

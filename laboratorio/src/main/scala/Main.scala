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
    val subscriptions: RDD[Subscription] = subscriptionOpts.flatMap {
      case Some(s) =>
        if (s.name.isEmpty || s.url.isEmpty) Iterator.empty
        else Iterator(s)
      case None => Iterator.empty
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
    //borramos una variable que guardaba un map con otodos los post, que ya o nescesitamos, y cambiamos todo para poder sacar lo que necesita
    val postsSuccess = downloadResults.count() 
    val postsFailed = subscriptions.count() - downloadResults.count()

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
}

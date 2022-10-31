import org.apache.spark.sql.{SaveMode, SparkSession}

object PremierLeague {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("premier_league")
      .master("local[1]")
      .getOrCreate()

    spark.conf.set("spark.cassandra.connection.host", sys.env("CASSANDRA_HOST"))
    spark.conf.set("spark.cassandra.connection.port", sys.env("CASSANDRA_PORT"))
    spark.conf.set("spark.cassandra.auth.username", sys.env("CASSANDRA_PASSWORD"))
    spark.conf.set("spark.cassandra.auth.password", sys.env("CASSANDRA_USERNAME"))

    spark.conf.set(s"spark.sql.catalog.cassadrancatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")

    val destination_dir = sys.env("DESTINATION_DIR")
    print(destination_dir)
    val teams_df = spark.read.json(destination_dir + "teams.json")
    val fixtures_df = spark.read.json(destination_dir + "fixtures.json")
    val events_df = spark.read.json(destination_dir + "events.json")

    //    Joining fixtures with their team names
    val fixtures_teams = fixtures_df
      .join(teams_df, fixtures_df("team_a") === teams_df("id"))
      .withColumnRenamed("name", "team_a_name")
      .select("team_a_name", "event", "team_h", "team_h_score", "team_a_score", "finished", "minutes", "kickoff_time", "started")
      .join(teams_df, fixtures_df("team_h") === teams_df("id"))
      .withColumnRenamed("name", "team_h_name")
      .select("team_a_name", "event", "team_h_name", "team_h_score", "team_a_score", "finished", "minutes", "kickoff_time", "started")
      .join(events_df.select("id", "name"), events_df("id") === fixtures_df("event"))
      .withColumnRenamed("name", "gameweek")
      .select("gameweek", "started", "finished", "kickoff_time", "minutes", "team_h_name", "team_a_name", "team_h_score", "team_a_score")
      .na.fill("2025-08-06T14:00:00Z", Array("kickoff_time"))
      .na.fill("Gameweek 0", Array("gameweek"))


    fixtures_teams.filter(fixtures_teams("gameweek") === "Gameweek 11").show()

    fixtures_teams
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "premier", "table" -> "fixtures_by_gameweek"))
      .mode(SaveMode.Append)
      .save()

  }
}
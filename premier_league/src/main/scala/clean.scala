import org.apache.spark.sql.SparkSession

object PremierLeague {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("premier_league")
      .master("local[1]")
      .getOrCreate()
    val destination_dir = "/home/user/Documents/projects/premier_league_data/data"
    val teams_df = spark.read.json(destination_dir + "/teams.json")
    val fixtures_df = spark.read.json(destination_dir + "/fixtures.json")

    val goals_a = fixtures_df
      .groupBy("team_a")
      .sum("team_a_score", "team_h_score")
      .withColumnRenamed("sum(team_a_score)", "away_goals_scored")
      .withColumnRenamed("sum(team_h_score)", "away_goals_conceded")
    val goals_h = fixtures_df
      .groupBy("team_h")
      .sum("team_h_score", "team_a_score")
      .withColumnRenamed("sum(team_h_score)", "home_goals_scored")
      .withColumnRenamed("sum(team_a_score)", "home_goals_conceded")

    val total_goals = goals_h
      .join(goals_a, goals_h("team_h") === goals_a("team_a"))
      .drop("team_a")
      .withColumnRenamed("team_h", "team")

    val total_goals_name = total_goals
      .join(teams_df, teams_df("code") === total_goals("team"))
      .select("code","short_name", "name", "home_goals_scored", "home_goals_conceded", "away_goals_scored",
      "away_goals_conceded")
    total_goals_name.show()

  }
}

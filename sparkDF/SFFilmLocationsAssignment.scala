// Databricks notebook source


// COMMAND ----------

// MAGIC %md #####Note: You have the freedom to use SQL statements, or DataFrame/Dataset operations to do the tasks 

// COMMAND ----------

// MAGIC %md (2 marks) Task 1. Read in the "Film Locations in San Francisco" data file and make it to a DataFrame called sfflDF.

// COMMAND ----------

val sfflDF = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/Film_Locations_in_San_Francisco.csv")

// COMMAND ----------

// MAGIC %md (2 marks) Task 2. Print the schema of sfflDF and show the first ten records.

// COMMAND ----------

sfflDF.printSchema
sfflDF.show(10)

// COMMAND ----------

// MAGIC %md (1 marks) Task 3. Count the number of records in sfflDF.

// COMMAND ----------

sfflDF.cache.count

// COMMAND ----------

// MAGIC %md (4 marks) Task 4. Filter the records where "director" is null, or there two or more director names (indicated by "and" or "&") in the "director" column, or "locations" is null, or "release year" is not a number (i.e. containing non-digit characters), and call the new DataFrame fsfflDF. 

// COMMAND ----------

import org.apache.spark.sql.functions._

sfflDF.createOrReplaceTempView("Film_Locations_in_San_Francisco")
val fsfflDF = spark.sql("select * from Film_Locations_in_San_Francisco")

fsfflDF.filter(isnull($"Director")|| $"Director".contains("&") || $"Director".contains("and") || isnull($"Locations") || $"Release Year".contains ("[a-zA-Z]") )
       .show

// COMMAND ----------

// MAGIC %md (2 marks) Task 5. Add a new column called "trimed_title" to fsfflDF that contains the title of films with the space trimed from the left and right end, drop the old "title" column and rename "trimed_title" column to "title", and call this new DataFrame csfflDF. 

// COMMAND ----------

import org.apache.spark.sql.functions._

val csfflDF = fsfflDF.withColumn("trimed_title", trim(fsfflDF("Title")))
                      .drop("Title")
                      .withColumnRenamed("trimed_title", "title")

csfflDF.show
        

// COMMAND ----------

// MAGIC %md #####Note: You will use csfflDF  in the following tasks

// COMMAND ----------

// MAGIC %md (2 marks) Task 6. Show the title, release year of films that were released between 2000-2009 (inclusive), ordered by the release year (from latest to earliest).

// COMMAND ----------

csfflDF.select("Title", "Release Year")
        .filter($"Release Year".between(2000, 2009))
        .orderBy(desc("Release Year"))
        .show
      

// COMMAND ----------

// MAGIC %md (2 marks) Task 7. Show the title of film(s) written by Keith Samples (note: there could be more than one writer name in the "writer" column)

// COMMAND ----------

import org.apache.spark.sql.functions._

csfflDF.select("Title")
        .filter($"Writer".contains("Keith Samples"))
        .show

// COMMAND ----------

// MAGIC %md (2 marks) Task 8. Show the earliest and latest release year. 

// COMMAND ----------

import org.apache.spark.sql.types.IntegerType

// chagne column type from string to integer
csfflDF.withColumn("Release Year", $"Release Year".cast(IntegerType))
       .agg(min("Release Year"), max("Release Year")).show

// COMMAND ----------

// MAGIC %md (3 marks) Task 9. Count the number of films, the number of distinct production company, and the average number of films made by a production company

// COMMAND ----------

csfflDF.agg(count("Title"), countDistinct("Production Company"), count("Title")/countDistinct("Production Company"))
        .show

// COMMAND ----------

// MAGIC %md (3 marks) Task 10. Show the title of films that were shot at more than three locations (inclusive).  

// COMMAND ----------

csfflDF.groupBy("Title")
        .agg(count("Locations"))
        .filter($"count(Locations)" >= 3)
        .select("Title")
        .show

// COMMAND ----------

// MAGIC %md (3 marks) Task 11. Add a new column called "Crew" to csfflDF that contains a list of people who had worked on the film (i.e. concatenate the column "Actor 1", "Actor 2", "Actor 3", "Director", "Distributor", Writer"). Show the title of films that Clint Eastwood were involved.

// COMMAND ----------

import org.apache.spark.sql.functions._

csfflDF.withColumn("Crew", concat_ws(",", $"Actor 1", $"Actor 2", $"Actor 3", $"Director", $"Distributor", $"Writer"))
        .filter($"Crew".contains("Clint Eastwood"))
        .select("Title")
        .distinct
        .show

// COMMAND ----------

// MAGIC %md (3 marks) Task 12. Show the number of films directed by each director, order by the number of films (descending).

// COMMAND ----------

csfflDF.groupBy("Director").count
        .orderBy(desc("count"))
        .show

// COMMAND ----------

// MAGIC %md (3 marks) Task 13. Show the number of films made by each production company and in each release year.

// COMMAND ----------

csfflDF.withColumn("Release Year", $"Release Year".cast(IntegerType))
        .groupBy("Production Company", "Release Year")
        .count
        .show

// COMMAND ----------

// MAGIC %md (3 marks) Task 14. Use csfflDF to generate a new DataFrame that has the column "title" and a new column "location_list" that contains an array of locations where the film was made, and then add a new column that contains the number of locations in "location_list". Show the first ten records of this dataframe.  

// COMMAND ----------

csfflDF.groupBy("Title") 
        .agg(collect_list("Locations") as "location_list")
        .withColumn("locations_num", size($"location_list"))
        .filter($"locations_num" > 0)
        .show(10)

// COMMAND ----------

// MAGIC %md (3 marks) Task 15. Use csfflDF to generate a new DataFrame that has the "title" column, and two new columns called "Golden Gate Bridge" and "City Hall" that contains the number of times each location was used in a film. Show the title of films that had used each location at least once.
// MAGIC Hint: Use pivot operation  

// COMMAND ----------

val locations = Seq("Golden Gate Bridge", "City Hall")
csfflDF.groupBy("Title")
        .pivot("Locations", locations)
        .agg(size(collect_list("Locations")))
        .filter($"Golden Gate Bridge" > 0 && $"City Hall" > 0)
        .show

// COMMAND ----------

// MAGIC %md (4 marks) Task 16. Add a new column to csfflDF that contains the names of directors with their surname uppercased.
// MAGIC Hint. Use a user defined function to uppercase the director's surname. Please refer to https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-sql-udfs.html

// COMMAND ----------

//https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-udfs.html
import org.apache.spark.sql.functions.udf

def upperSurname (directors: String) = {
  var tempNames = directors.replace(" and ", ",").replace("&", ",")
  var names = Array[String]()
  var newNames = Array[String]()
  
   // more than one directors
   if(tempNames.contains(",")){
      tempNames.split(",")
                .map(name => {
                   val pos = name.lastIndexOf(" ")
                   val firstName = name.substring(0, pos)
                   val surName = name.substring(pos).toUpperCase
                   (firstName + surName)
               }).toList.mkString(",")
  }else{
    // onlly one director
    val pos = directors.lastIndexOf(" ")
     if(pos > 0){
        val firstName = directors.substring(0, pos)
        val surName = directors.substring(pos).toUpperCase
        (firstName + surName)
     }else{
       directors
     }     
  }
}

val upperNameUDF = udf(upperSurname _)

csfflDF.withColumn("UpperSurname", $"Director")
        .withColumn("UpperSurname", upperNameUDF($"UpperSurname"))
        .select("Director", "UpperSurname")
        .distinct
        .show


// COMMAND ----------

// MAGIC %md (4 marks) Task 17. Use csfflDF to generate a new DataFrame that has the column "Locations" and a new column "Actors" that contains a list of distinct actors who had cast in a location. 
// MAGIC Note: run "Hint" below if you need a bit help on this task  

// COMMAND ----------

import org.apache.spark.sql.functions.udf

val distinctUDF = udf{(actors: Seq[String]) => actors.distinct}

csfflDF.groupBy("Locations")
        .agg(collect_set("Actor 1") as "Actor1", 
             collect_set("Actor 2") as "Actor2",
             collect_set("Actor 3") as "Actor3")
        //concate multiple actor columns, then distinct
        .withColumn("Actors", concat($"Actor1", $"Actor2", $"Actor3"))
        .withColumn("Actors",distinctUDF($"Actors"))
        .select("Locations", "Actors")
       .show

// COMMAND ----------

displayHTML("<p>One solution is groupBy on <i>Locations</i>, then collect_set on <i>Actor 1</i>, <i>Actor 2</i>, <i>Actor 3</i>, finally define a user function to combine <i>collect_set(Actor 1)</i>, <i>collect_set(Actor 2)</i>,  <i>collect_set(Actor 3)</i></p>")

// COMMAND ----------

// MAGIC %md (4 marks) Task 18. Show the names of people who is an actor (in one of "Actor 1", "Actor 2" and "Actor 3" column) and also a director (in the "Director" column). Note: run "Hint" below if you need a bit help on this task

// COMMAND ----------

val actorsDF = csfflDF.select("Actor 1", "Title")
              .union(csfflDF.select("Actor 2", "Title"))
              .union(csfflDF.select("Actor 3", "Title"))
val dirctorDF = csfflDF.select("Director", "Title")

actorsDF.join(dirctorDF, actorsDF("Title") === dirctorDF("Title"))
        .filter($"Actor 1" === $"Director")
        .distinct
        .withColumnRenamed("Actor 1", "Actor/Director")
        .select("Actor/Director")
        .show



// COMMAND ----------

displayHTML("<p>One solution is use csfflDF to create four new DataFrames (for 'Actor 1', 'Actor 2', 'Actor 3' and 'Director') that contains two columns, the actor/director name and the film title, and then use 'union' and 'join' opetations.</p>")

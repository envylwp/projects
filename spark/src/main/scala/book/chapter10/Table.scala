package book.chapter10

object Table {


  def main(args: Array[String]): Unit = {
    /**
      *
    CREATE TABLE flights(
      DEST_COUNTRY_NAME STRING,ORIGIN_COUNTRY_NAME STRING,count LONG)
    USING JSON OPTIONS(path'/data/flight-data/json/2015-summary.json')


      CREATE TABLE flights_csv(
        DEST_COUNTRY_NAME STRING,
        ORIGIN_COUNTRY_NAME STRING COMMENT"remember, the US will be most prevalent",
        count LONG)
        USING csv OPTIONS(headertrue,path'/data/flight-data/csv/2015-summary.csv')


      CREATE TABLE flights_from_select USING parquet AS SELECT * FROM flights

      CREATE TABLE IF NOT EXISTS flights_from_select
      AS SELECT * FROM flights
      */

    println("------------------------------Creating External Tables------------------------------------")

    /**

    CREATE EXTERNAL TABLE hive_flights(
    DEST_COUNTRY_NAME STRING,ORIGIN_COUNTRY_NAME STRING,count LONG)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION'/data/flight-data-hive/'

      CREATE EXTERNAL TABLE hive_flights_2
      ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
      LOCATION '/data/flight-data-hive/' AS SELECT * FROM flights



    println("------------------------------Inserting into Tables------------------------------------")
        INSERT INTO flights_from_select
        SELECT DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME,count FROM flights LIMIT 20

      INSERT INTO partitioned_flights
      PARTITION(DEST_COUNTRY_NAME="UNITED STATES")
      SELECT count,ORIGIN_COUNTRY_NAME FROM flights
      WHERE DEST_COUNTRY_NAME='UNITED STATES' LIMIT 12




      */

  }
}

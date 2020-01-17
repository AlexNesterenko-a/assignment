ECommerce Analytics
===================

These analytics are build from input data sample located as csv file in resources folder.

The goal is to enrich incoming data by emitting different sessions based on users, categories and time spent

After enrichment the following statistics are computed:

1) Median session duration for each category
2) For each category find # of unique users spending less than 1 min, 1 to 5 mins and more than 5 mins
3) Finding top n products ranked by time spent by users on product pages

Enrichment is implemented in 2 ways:
* Spark DataFrame API window functions approach
* Spark [user defined aggregate functions](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/expressions/UserDefinedAggregateFunction.html)

Statistics calculations are implemented with Spark DataFrame API and Spark SQL approaches

Unit tests implemented to check whether produced results are correct based on
reduced input samples and matching with precalculated results on these chunks.

By default app is running in Spark standalone mode. Number of cores engaged and driver max RAM can be specified in
config file, default: 4 cores, 2GB RAM

Application is developed using Scala 2.12.8 and Apache Spark 2.4.3
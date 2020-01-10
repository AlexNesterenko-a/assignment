Marketing Analytics
===================

This app builds purchases attribution projection.

This projection is build from input data samples located as csv files in resources folder.

The goal is to match actual purchases with click stream data from mobile app.
We assume that user session starts with app_open event and ends with app_close event,
so each purchase is associated with no more than one session.
There could be sessions without purchases as normal.

Top campaigns and top channels are calculated based on aforesaid projection.

Each calculation is done by both plain sql and DataFrame/Dataset API.

Unit tests check whether results of both calculations for each task are equal.

By default app is running in Spark standalone mode. Number of cores engaged and driver max RAM can be specified in
config file, default: 4 cores, 2GB RAM

Application is developed using Scala 2.12.8 and Apache Spark 2.4.3
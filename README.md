udaf-concat_distinct
====================
Hive UDAF example

How to create the function in hive:

    hive> ADD JAR /full/path/to/udaf-concat_distinct-1.0.jar;
    hive> CREATE TEMPORARY FUNCTION concat_distinct AS 'hu.radoop.udaf.GenericUDAFConcatDistinct';

Performs aggregation over each group. Returns a string of the objects separated by a comma and a space. One value will only appear once.
Almost like SQL "GROUP_CONCAT(DISTINCT expr)" or Hive "concat_ws( ', ', collect_set(expr))", although the order of the output can be different with this function.
Usage: SELECT att2, concat_distinct(att3) FROM table1 GROUP BY att2")

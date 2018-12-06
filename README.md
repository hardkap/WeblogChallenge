# WeblogChallenge
This is an interview challenge for Paytm Labs. 

## Run environment:
1. This was tested in databricks community edition - notebook environment. WebLog-Notebook.html is the output from this run.

2. This was also tested in spark-shell (Spark 2.2.1). Refer to WebLog.scala

## Running in spark-shell:

1. Run spark-shell from the command line (assuming Spark 2.2.1 is installed and spark-shell is available in the path)

2. In the spark-shell, load the scala file.
```:load WebLog.scala```

3. Run the main function. Pass the location of the weblog file and the IP for which the 2nd and 3rd prediction needs to be done. This will run all other functions and generate the outputs for all the challenges.
```WebLog.main(Array("/path/to/web_log.gz", "103.24.23.10"))

## Outputs:
```
Analytics Challenge-1 ==> Sessionize by IP:
+---------------+-------------+--------------------+--------------------+
|     session_id|     clientIp|       session_start|         session_end|
+---------------+-------------+--------------------+--------------------+
|  1.186.41.12_0|  1.186.41.12|2015-07-22 21:16:...|2015-07-22 21:16:...|
|   1.186.41.1_1|   1.186.41.1|2015-07-22 18:11:...|2015-07-22 18:13:...|
|  1.186.76.11_1|  1.186.76.11|2015-07-22 17:42:...|2015-07-22 18:17:...|
| 1.187.110.59_0| 1.187.110.59|2015-07-22 10:44:...|2015-07-22 10:44:...|
|1.187.180.126_0|1.187.180.126|2015-07-22 09:12:...|2015-07-22 09:12:...|
|1.187.228.210_0|1.187.228.210|2015-07-22 16:21:...|2015-07-22 16:35:...|
| 1.187.228.88_0| 1.187.228.88|2015-07-22 10:32:...|2015-07-22 10:45:...|
| 1.22.187.226_0| 1.22.187.226|2015-07-22 17:53:...|2015-07-22 17:53:...|
| 1.23.101.102_0| 1.23.101.102|2015-07-22 16:49:...|2015-07-22 16:55:...|
|  1.23.226.88_0|  1.23.226.88|2015-07-22 16:43:...|2015-07-22 16:55:...|
+---------------+-------------+--------------------+--------------------+
only showing top 10 rows

Analytics Challenge-2 ==> Average Session Length: 574.5947358163104 (secs)
Analytics Challenge-3 ==> Unique URL visits by Session:
+---------------+-------------+-------------+
|     session_id|     clientIp|distinct_urls|
+---------------+-------------+-------------+
|  1.186.41.12_0|  1.186.41.12|            1|
|   1.186.41.1_1|   1.186.41.1|            3|
|  1.186.76.11_1|  1.186.76.11|           17|
| 1.187.110.59_0| 1.187.110.59|            1|
|1.187.180.126_0|1.187.180.126|            1|
|1.187.228.210_0|1.187.228.210|            5|
| 1.187.228.88_0| 1.187.228.88|            5|
| 1.22.187.226_0| 1.22.187.226|            1|
| 1.23.101.102_0| 1.23.101.102|            6|
|  1.23.226.88_0|  1.23.226.88|            2|
+---------------+-------------+-------------+
only showing top 10 rows

Analytics Challenge-4 ==> Most Engaged IPs:
+--------------+---------------------+
|      clientIp|session_length (secs)|
+--------------+---------------------+
| 220.226.206.7|               4097.0|
|  52.74.219.71|               3042.0|
| 119.81.61.166|               3039.0|
|  52.74.219.71|               3030.0|
| 54.251.151.39|               3029.0|
|103.29.159.186|               3024.0|
| 119.81.61.166|               3022.0|
| 123.63.241.40|               3009.0|
| 202.134.59.72|               3008.0|
|103.29.159.138|               3007.0|
+--------------+---------------------+
only showing top 10 rows

Machine Learning Challenge-1 ==> Requests/sec in the next Minute: 12
Machine Learning Challenge-2 ==> Session Length for a given IP (including previous sessions):
+---------+--------------+
|  session|session_length|
+---------+--------------+
| existing|         966.0|
| existing|        2010.0|
| existing|         392.0|
|predicted|         863.0|
+---------+--------------+

Machine Learning Challenge-3 ==> Unique Urls for a given IP (including previous sessions):
+---------+-------------+
|  session|distinct_urls|
+---------+-------------+
| existing|         39.0|
| existing|          9.0|
| existing|          2.0|
|predicted|         18.0|
+---------+-------------+

```

val log = sc.textFile("sparkinput/test_log")

val IPPair = log.map{line =>
     | val p = line.split(" ")
     | (p(0),1)
     | }

val AddressPair = log.map{line =>
     | val p = line.split(" ")
     | (p(6),1)
     | }

val IPCount = IPPair.reduceByKey(_+_)
val AddressCount = AddressPair.reduceByKey(_+_)


spark.time(IPCount.collect().maxBy(_._2))
Time taken: 4829 ms
res5: (String, Int) = (10.216.113.172,158614)

spark.time(AddressCount.collect().maxBy(_._2))
Time taken: 4751 ms
res6: (String, Int) = (/assets/css/combined.css,117348)

val log = sc.textFile("sparkinput/test_log").cache()

spark.time(IPCount.collect().maxBy(_._2))
Time taken: 885 ms
res7: (String, Int) = (10.216.113.172,158614)

spark.time(AddressCount.collect().maxBy(_._2))
Time taken: 253 ms
res8: (String, Int) = (/assets/css/combined.css,117348)



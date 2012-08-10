_cass=target/lib/cassandra-all-1.0.9.jar 


/Library/hadoop-1.0.2/bin/hadoop jar  target/medline-0.0.1-SNAPSHOT.jar edu.uiowa.icts.hadoop.MedlineMeshCount -libjars $_cass


# IoT-simulation

Integrazione di Apache Kafka, Apache Spark ed Apache Cassandra.


In questo progetto è stata simulata l'acquisizione di una massiva quantità di dati da alcuni dispositivi.

I dispositivi non sono altro che dei Kafka Producer che con una elevata frequenza trasmettono messaggi verso un Kafka Broker.
Il Broker è diviso in appositi topic per ogni dispositivo in esecuzione.

Un insieme di Kafka Consumer recupera i messaggi dal Broker e li elabora facendo delle aggregazioni temporali mediante l'uso di PySpark.

I dati elaborati vengono salvati in maniera persistente su un database NoSQL: Apache Cassandra.

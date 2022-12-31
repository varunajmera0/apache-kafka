# 👊 Apache Kafka Producers Advance

Deep dive into Kafka producers configs

- **Kafka producers only write data to the current leader broker for a partition.**

In this section, we will discuss some of the ones we judge are the most important:

### Configs
1. [acks](https://github.com/varunajmera0/apache-kafka/kafka-produces/advance/md_files/acks "Acks Config")
2. [retries](https://github.com/varunajmera0/apache-kafka/kafka-produces/advance/md_files/retries "Retries Config")
3. [batch.size](https://github.com/varunajmera0/apache-kafka/kafka-produces/advance/md_files/batch.size "Batch Size Config")
4. [enable.idempotence](https://github.com/varunajmera0/apache-kafka/kafka-produces/advance/md_files/enable.idempotence "Enable Idempotence Config")
5. [compression.type](https://github.com/varunajmera0/apache-kafka/kafka-produces/advance/md_files/enable.idempotence "Compression Type Config")

### Instructions
I have implemented `acks` and `retries` configs. You can use [this](https://github.com/varunajmera0/apache-kafka/kafka-produces/advance).

 - It will create topic automatically. Config parameters in [.env](https://github.com/varunajmera0/apache-kafka/kafka-produces/advance/.env).
 - Run Zookeepeer
   > bin/zookeeper-server-start.sh config/zookeeper.properties
 - Run Brokers - Copy these [brokers files](https://github.com/varunajmera0/apache-kafka/kafka-produces/brokers) in config folder of kafka.
   > bin/kafka-server-start.sh config/server0.properties
   >
   > bin/kafka-server-start.sh config/server1.properties
   > 
   > bin/kafka-server-start.sh config/server2.properties
 - Install Dependencies
   > pip3 install -r [requirements.txt](https://github.com/varunajmera0/apache-kafka/kafka-produces/requirements.txt "Requirements File")
 - Run FastAPI Server
   > uvicorn adv_topic_producer:app --reload
 - Hit this API in Postman/Insomnia
   > http://127.0.0.1:8000/api/people
   > 
   > send this input in body
   > 
   > {"count": 3 }
 - After sometime, bring down 2 brokers using `CTRL+C` and hit that API again. You will get an error `NotEnoughReplicasException`.

Same way, you can use [simple broker](https://github.com/varunajmera0/apache-kafka/kafka-produces/basic) example.

 

Credits: 
1. [Conduktor](https://www.conduktor.io/kafka/kafka-producers-advanced) <br>
2. https://towardsdatascience.com/10-configs-to-make-your-kafka-producer-more-resilient-ec6903c63e3f
3. https://kalpads.medium.com/design-aspects-of-resilient-event-driven-applications-using-apache-kafka-d0b1c03e16e1

> Happy Coding! :v:

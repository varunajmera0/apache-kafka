# 👊 Apache Kafka Producers Simple

### Instructions
You can use [this](https://github.com/varunajmera0/apache-kafka/kafka-produces/basic).

 - It will create topic automatically. Kafka config parameters in [.env](https://github.com/varunajmera0/apache-kafka/kafka-produces/basic/.env).
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
   > uvicorn basic_topic_producer:app --reload
 - Hit this API in Postman/Insomnia
   > http://127.0.0.1:8000/api/people
   > 
   > send this input in body
   > 
   > {"count": 3 }

Credits: 
1. [Conduktor](https://www.conduktor.io/kafka/kafka-producers-advanced) <br>
2. https://towardsdatascience.com/10-configs-to-make-your-kafka-producer-more-resilient-ec6903c63e3f
3. https://kalpads.medium.com/design-aspects-of-resilient-event-driven-applications-using-apache-kafka-d0b1c03e16e1

> Happy Coding! :v:

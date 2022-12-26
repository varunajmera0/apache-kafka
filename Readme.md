# 🚀 Apache Kafka

We will try to simulate it with the help of real-world examples.

## What is Apache Kafka?

Apache Kafka is just like any mall that decouples seller and consumer.

> Apache Kafka allows us to decouple data streams and systems. With Apache Kafka as a data integration layer, data sources will publish their data to Apache Kafka and the target systems will source their data from Apache Kafka.

## Kafka Analogy

1. Source System/Application = Seller(clothier, grocer) | Souce system(web, mobile and cloud)
2. Producer = Who sell the goods i.e. seller(outside of mall)
3. Topic = Goods Name (Clothes, Food)
4. Brokers = Employee @ each section of Mall shop
5. Partition = Similar to category/section. Goods are broken down into sections/categories/partitions. For example -
   1. Men section,
   2. Women section,
   3. Veg section,
   4. Non-veg section
6. Offset = Index/Rack No
7. Key = Label like men, woman, veg, non-veg
8. Message = Label + Goods
9. Compression Type = Packing of goods(Box, gift, parcel)
10. Header = Metadata of Goods
11. Timestamp = A timestamp is added either by the user or the system in the message
12. Message Serializers = Representation of object(Label + Goods) in special format
13. Consumer = Who consumes the Goods i.e. customers

## Reference

1. Seller - S1 & S2
   1. S1 sells clothes i.e. clothiers.
   2. S2 sells food i.e. grocer.
2. Mall shops - MS1 & MS2
   1. MS1 is a clothes shop in the mall.
   2. MS2 is a food shop in the mall.

## Source System/Application

Who generates/produces data/goods

> In the real world, the Seller(clothier, grocer) is the application.

> In technology, Souce system(web, mobile and cloud) is application.

## Topic

Where data/goods go. Data/Goods will only move to the particular allotment.

> Web application is allocated to the web topic and mobile application is allocated to the mobile topic.

> In the same way, clothier goods will only move to the MS1(clothes shop) and grocer goods will move to MS2(Food Shop).

> Kafka topics are immutable: once data is written to a partition, it cannot be changed. t is like a non-returnable item.

## PARTITIONS

Broken down into a number of partitions

> Similar to category/section. Goods/Data are broken down into sections/categories/partitions. Here we are taking category/section for explaining purposes but in Kafka, partitions are numbered starting from 0 to N-1, where N is the number of partitions.

## Kafka Offsets & Ordering

Position of items where items are kept.

> If men's jeans are placed in rack1 of the man category at 3rd position. It is not necessary that in rack1 of the woman's category women’s jeans will be in 3rd position. It may be at rack2 at the 5th position also.

> Likewise in Kafka, If a topic has more than one partition, Kafka guarantees the order of messages within a partition, but there is no ordering of messages across partitions.

## Producer

Who sells the goods to mall/Applications that send data into topics are known as Kafka producers

> clothier who sends goods into MS1 is known as a Kafka producer. clothier typically integrated via that mall to sell goods to MS1.

> Likewise in Kafka, Applications that send data into topics are known as Kafka producers. Applications typically integrate a Kafka client library to write to Apache Kafka.

> In the real world, the seller & buyer gives some kind of acknowledgment(acks) for goods delivery. Same way in Kafka also producer must specify a level of acknowledgment(acks) for a message to be successfully written into a Kafka topic.

## Message

The message is nothing but Label + Goods. In Kafka Key + Value

## Kafka Message Anatomy

The producer creates Kafka messages. A Kafka message consists of the following elements:

![Kafka Message Structure](/assets/kafkamsg.webp)

## Message Keys

Each event message contains an optional key and a value.

> In case label/key (label/key=null) is not specified by the producer, goods are distributed evenly across categories in a round-robin fashion in the particular allotment. Suppose,

```
1. key=null, man jean comes the first time it will go to man category.
2. key=null, woman jeans come the second time it will go to woman category.
3. key=null, woman T-shirt comes 3rd time it will go to man category.
```

Likewise in Kafka, messages are distributed evenly across partitions in a topic. This means messages are sent in a round-robin fashion (partition p0 then p1 then p2, etc... then back to p0, and so on...).

> In case label/key (label/key=null) is specified by the producer.
> All goods that share the same label will always be sent and stored in the same category.

```
1. key=man, man jean comes the first time it will go to man category.
2. key=woman, woman jeans come the second time it will go to woman category.
3. key=woman, woman T-shirt comes 3rd time it will go to woman category.
```

all messages that share the same key will always be sent and stored in the same Kafka partition.

> Kafka message keys are commonly used when there is a need for message ordering for all messages sharing the same field.

## Message Value

The goods/content of the message can be null.

## Message Compression Type

Packing of goods(Box, gift, parcel)/Compressed

> The compression type can be specified as part of the message. Options are none, gzip, lz4, snappy, and zstd.

## Message Headers

Metadata of goods/message

> Specify metadata about the goods/message, especially for tracing.

## Message Topic + Partition + Offset

The combination of topic + partition + offset uniquely identifies the message.

## Kafka Message Serializers

Representation of the object(Label(Key) + Goods(Message)) in special format

> Just like, In clothes shop introduce the special format of goods and employee understands that special format for items @ each section of MS1.

> Likewise in Kafka, Kafka brokers expect byte arrays as keys and values of messages.

```
We have a message with an Integer key and a String value. Since the key is an integer,
we have to use an IntegerSerializer to convert it into a byte array.
For the value, since it is a string, we must leverage a StringSerializer.
```

## Message Key Hashing

Key Hashing is the process of determining the mapping of a key to a partition

> Clothier defines such a mechanism that mechanism will determine which goods to which mall shop.

> A Kafka partitioner is a code logic that takes a record and determines to which partition to send it.

## Consumers

Who consume the Goods/Data

> When you go to the mall and purchase those clothes according to your need. It is called a consumer. In the same way, Applications that read data from Kafka topics are known as consumers.

> Consumers typically integrated via that the mall to consume goods from MS1. Applications integrate a Kafka client library to read from Apache Kafka.

`Note: Clothier & Consumer are not part of the mall. They are connected through the mall. Likewise, Kafka producers and consumers are also connected through the Kafka client library to read from/ write to Apache Kafka`.

Credits - [Conduktor](https://www.conduktor.io/kafka) | [Stéphane Maarek](https://www.linkedin.com/in/stephanemaarek)

> Happy Coding! :v:

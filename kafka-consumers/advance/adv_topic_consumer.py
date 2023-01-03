import json, logging, os
import signal
import sys

import confluent_kafka
from confluent_kafka import KafkaException, TopicPartition
from confluent_kafka.admin import ConfigResource as ConfluentConfigResource, ConfigSource as ConfluentConfigSource
from dotenv import load_dotenv
# from kafka import TopicPartition, OffsetAndMetadata, ConsumerRebalanceListener

from kafka.consumer import KafkaConsumer
# from kafka.client_async import
from kafka.coordinator.assignors.sticky.sticky_assignor import StickyPartitionAssignor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

load_dotenv(verbose=True)

def people_key_deserialize(key):
    return key.decode("utf-8")

def people_value_deserialize(value):
    return json.loads(value.decode("utf-8"))

# class MyConsumerRebalanceListener(ConsumerRebalanceListener):
#
#
#     def on_partitions_revoked(self, revoked):
#         print("Partitions %s revoked" % revoked)
#
#     def on_partitions_assigned(self, assigned):
#         print("Partitions %s assigned" % assigned)

def on_partitions_revoked(consumer, partitions):
    # print(f"Partitions {partitions} revoked {consumer}")
    print(dir(consumer))
    # q
    consumer.incremental_unassign(partitions)
    print("Partitions %s revoked" % partitions)

def on_partitions_assigned(consumer, partitions):
    # print(f"Partitions {partitions} revoked {consumer}")
    # q
    consumer.incremental_assign(partitions)
    print("Partitions %s assigned" % partitions)

class Killer:
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.shutdown_signal = False

    def exit_gracefully(self, signal_no, stack_frame):
        self.shutdown_signal = True
        raise SystemExit

def print_config(config, depth):
    # print('%10s = %-10s  [%s,is:read-only=%r,default=%r,sensitive=%r,synonym=%r,synonyms=%s]' %
    #       ((' ' * depth) + config.name, config.value, ConfluentConfigSource(config.source),
    #        config.is_read_only, config.is_default,
    #        config.is_sensitive, config.is_synonym,
    #        ["%s:%s" % (x.name, ConfluentConfigSource(x.source))
    #         for x in iter(config.synonyms.values())]))

    for x in iter(config.synonyms.values()):
        print(x)


def example_describe_configs():
    """ describe configs """
    from confluent_kafka.admin import AdminClient
    kafka_admin = AdminClient({"bootstrap.servers": os.environ['BOOTSTRAP_SERVERS']})
    fs = kafka_admin.describe_configs([ConfluentConfigResource(confluent_kafka.admin.RESOURCE_TOPIC, os.environ['TOPICS_ADV_NAME'])])


    # Wait for operation to finish.
    for res, f in fs.items():
        try:
            configs = f.result()
            for config in iter(configs.values()):
                print_config(config, 1)

        except KafkaException as e:
            print("Failed to describe {}: {}".format(res, e))
        except Exception:
            raise

def main():
    logger.info(f"""
        Started Kafka consumer
        for topic {os.environ['TOPICS_ADV_NAME']}
    """)
    print("sys.argv", sys.argv)

    from confluent_kafka import Consumer

    c = Consumer({
        'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
        'group.id': os.environ['CONSUMER_GROUP'],
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'partition.assignment.strategy': 'cooperative-sticky',
        # 'session.timeout.ms': 10000,
        # 'group.instance.id': 'grpup-'+sys.argv[1]
    })

    c.subscribe([os.environ['TOPICS_ADV_NAME']], on_assign=on_partitions_assigned, on_revoke=on_partitions_revoked)
    # print("TopicPartition(os.environ['TOPICS_ADV_NAME'])", TopicPartition(os.environ['TOPICS_ADV_NAME'], partition=1).partition)
    # c.incremental_assign([TopicPartition(os.environ['TOPICS_ADV_NAME'], partition=1).partition])

    killer = Killer()
    while not killer.shutdown_signal:
        try:
            while True:
                # logger.info("Polling.....")
                msg = c.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue

                print('Received message: {}'.format(msg.value().decode('utf-8')))
        except SystemExit:
            logger.info('Shutting down...')
        finally:
            c.close()
            logger.info('The consumer is nw gracefully closed...')



    # fs = example_describe_configs()
    # logger.info("fs" + str(fs))
    # listener = MyConsumerRebalanceListener()
    # consumer = KafkaConsumer(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
    #                         group_id=os.environ['CONSUMER_GROUP'],
    #                         key_deserializer=people_key_deserialize,
    #                         value_deserializer=people_value_deserialize,
    #                          enable_auto_commit=False,
    #                          partition_assignment_strategy=[StickyPartitionAssignor]
    #                          )
    # consumer.subscribe([os.environ['TOPICS_ADV_NAME']], listener=listener)
    # killer = Killer()
    # while not killer.shutdown_signal:
    #     try:
    #         while True:
    #             logger.info("Polling.....")
    #             # message_batch = consumer.poll(timeout_ms=1000)
    #
    #             for record in consumer:
    #             # for record in message_batch:
    #                 logger.info(f"""
    #                     Consumed person {record.value}
    #                     with key '{record.key}'
    #                     from partition {record.partition}
    #                     at offset {record.offset}
    #                 """)
    #                 topic_partition = TopicPartition(record.topic, record.partition)
    #                 offset = OffsetAndMetadata(record.offset+1, record.timestamp)
    #
    #                 consumer.commit({topic_partition: offset})
    #     except SystemExit:
    #         logger.info('Shutting down...')
    #     finally:
    #         consumer.close()
    #         logger.info('The consumer is nw gracefully closed...')



if __name__ == "__main__":
    main()
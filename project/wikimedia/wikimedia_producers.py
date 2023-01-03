import json
import logging
import os

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

load_dotenv(verbose=True)

from sseclient import SSEClient as EventSource

url = 'https://stream.wikimedia.org/v2/stream/recentchange'


def create_topic():
    client = AdminClient({'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS']})
    topic = NewTopic(os.environ['TOPICS_NAME'],
                     num_partitions=int(os.environ['TOPICS_PARTITIONS']),
                     replication_factor=int(os.environ['TOPICS_REPLICAS']),
                     config={"min.insync.replicas": 2})
    try:
        futures = client.create_topics([topic])
        for topic_name, future in futures.items():
            future.result()
            logger.info(f"Create topic {topic_name}")
    except KafkaException as e:
        logger.warning("Topic already exist: " + str(e))
    except Exception as e:
        logger.warning(e)
    finally:
        logger.info(f"Closing....")


def call_producer():
    return Producer({'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
                     'acks': 'all',
                     'linger.ms': 20,
                     'batch.size': 32 * 1024,
                     'compression.type': 'snappy'})


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
		Triggered by poll() or flush().
	"""
    if err is not None:
        logger.error('Message delivery failed: {}'.format(err))
    else:
        logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def main():
    create_topic()
    producer = call_producer()
    for event in EventSource(url, last_id=None):
        if event.event == 'message':
            try:
                data_dict = json.loads(event.data)
                key = data_dict["wiki"].encode('utf-8')
                logger.info('Message delivered to {} [{}]'.format(key, data_dict))
                producer.produce(topic=os.environ['TOPICS_NAME'],
                                 key=key,
                                 value=event.data.encode('utf-8'),
                                 callback=delivery_report)
                producer.poll(1)
            except ValueError:
                pass
            else:
                pass
    producer.flush()


if __name__ == "__main__":
    main()
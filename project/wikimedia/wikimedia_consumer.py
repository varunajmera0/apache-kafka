
# //first create openserach client
# //create kafka consumer
# //main logic
# //close
import json
import logging
import os
import signal
import time

from confluent_kafka import Consumer
from dotenv import load_dotenv

from opessearch import ElasticSearch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

load_dotenv(verbose=True)

class Killer:
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.shutdown_signal = False

    def exit_gracefully(self, signal_no, stack_frame):
        self.shutdown_signal = True
        raise SystemExit


def main():
    c = Consumer({
        'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
        'group.id': os.environ['CONSUMER_GROUP'],
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'partition.assignment.strategy': 'cooperative-sticky',
        # 'session.timeout.ms': 10000,
        # 'group.instance.id': 'grpup-'+sys.argv[1]
    })

    c.subscribe([os.environ['TOPICS_NAME']])
    es = ElasticSearch()
    logger.info(es.create_index())
    killer = Killer()
    bulk_data = []
    while not killer.shutdown_signal:
        try:
            while True:
                logger.info("Polling.....")
                msg = c.poll(3.0)

                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue
                # id = msg.topic() + "_" + str(msg.partition()) + "_" + str(msg.offset())
                # print("record", msg.partition())
                print('Received message: {}'.format(msg.value().decode('utf-8')))
                id = json.loads(msg.value().decode('utf-8')).get("meta").get("id")
                logger.info(f"record id: {id}")
                # bulk request
                bulk_data.append(
                    {
                        "_index": "wikimedia",
                        "_id": id,
                        "_source": json.loads(msg.value().decode('utf-8'))
                    }
                )
                if len(bulk_data) > 10:
                    bulk_response = es.bulk_create(bulk_data)
                    logger.info(f"Opensearch bulk response: {bulk_response}")
                    bulk_data = []
                    c.commit()
                    logger.info(f"Offset have been committed!")
                    time.sleep(10)
                # single single request
                # doc = es.create_document(document=msg.value().decode('utf-8'), id=id)
                # logger.info(f"Opensearch document id: {doc.get('_id') if doc is not None else None}")
                # c.commit()
                # logger.info(f"Single message offset have been committed!")


        except SystemExit:
            logger.info('Shutting down...')
        finally:
            c.close()
            logger.info('The consumer is nw gracefully closed...')

if __name__ =="__main__":
    main()
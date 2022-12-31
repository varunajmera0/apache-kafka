import json
import os
import random
import time
import uuid
from typing import List

from dotenv import load_dotenv
from faker import Faker
from fastapi import FastAPI
import logging

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError
from commands import CreatePeopleCommand
from entities import Person

logging.basicConfig(level=logging.INFO)


logger = logging.getLogger("Advance Kafka")
load_dotenv(verbose=True)

app = FastAPI()


def flatten(d, sep="_"):
    import collections
    obj = collections.OrderedDict()

    def recurse(t, parent_key=""):
        if isinstance(t, list):
            for i in range(len(t)):
                recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k, v in t.items():
                recurse(v, parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t
    recurse(d)
    return obj

@app.on_event('startup')
async def startup_event():

    # single topic create
    client = KafkaAdminClient(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])
    topic = NewTopic(name=os.environ['TOPICS_ADV_NAME'],
                     num_partitions=int(os.environ['TOPICS_ADV_PARTITIONS']),
                     replication_factor=int(os.environ['TOPICS_ADV_REPLICAS']),
                     topic_configs={"min.insync.replicas": 2}
                     )

    try:
        # topic already exist or not
        topics_list = client.list_topics()
        print("List of topics: ", topics_list)
        # create topic
        client.create_topics([topic])
        print("After created, list of topics: ", client.list_topics())
        configs = client.describe_configs(
            config_resources=[ConfigResource(ConfigResourceType.TOPIC, os.environ['TOPICS_ADV_NAME'])],
            include_synonyms=True)

        items = configs[0].to_object()
        """
        schema
        SCHEMA = Schema(
            ('throttle_time_ms', Int32),
            ('resources', Array(
                ('error_code', Int16),
                ('error_message', String('utf-8')),
                ('resource_type', Int8),
                ('resource_name', String('utf-8')),
                ('config_entries', Array(
                    ('config_names', String('utf-8')),
                    ('config_value', String('utf-8')),
                    ('read_only', Boolean),
                    ('config_source', Int8),
                    ('is_sensitive', Boolean),
                    ('config_synonyms', Array(
                        ('config_name', String('utf-8')),
                        ('config_value', String('utf-8')),
                        ('config_source', Int8)))))))
        )
        """
        with open("default_advance_config.json", "w") as file:
            file.write(json.dumps(dict(flatten(items)), indent=4))

    except TopicAlreadyExistsError as e:
        logger.warning("Topic already exist: " + str(e))
    finally:
        client.close()


class SuccessHandler:
    def __init__(self, person):
        self.person = person

    def __call__(self, metadata):
        logger.info(f"""
            Successfully produced
            person {self.person}
            to topic {metadata.topic}
            and partition {metadata.partition}
            at offset {metadata.offset}
        """)


class ErrorHandle:
    def __init__(self, person):
        self.person = person

    def __call__(self, ex):
        logger.error(f"Failed producing person - {self.person}", exc_info=ex)


@app.get("/hello-world")
async def hello_world():
    return {"messgae": "Hello world!!"}


def call_producer():
    return KafkaProducer(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
                         max_in_flight_requests_per_connection=int(os.environ['TOPICS_ADV_INFLIGHT_REQS']),
                         linger_ms=int(os.environ['TOPICS_ADV_LINGER_MS']),
                         acks=os.environ['TOPICS_ADV_ACK'],
                         retries=int(os.environ['TOPICS_ADV_RETRIES']),
                         )

@app.post("/api/people", status_code=201, response_model=List[Person])
async def create_person(cmd: CreatePeopleCommand):
    people: List[Person] = []
    faker = Faker()
    producer = call_producer()
    for i in range(cmd.count):
        person = Person(id=str(uuid.uuid4()), name=faker.name(), title=faker.job().title())
        people.append(person)
        producer.send(topic=os.environ['TOPICS_ADV_NAME'],
                      key=person.title.lower().replace(r's+', '-').encode('utf-8'),
                      value=person.json().encode('utf-8')
                      ) \
            .add_callback(SuccessHandler(person)) \
            .add_errback(ErrorHandle(person))
    producer.flush()
    return people

import json
import os
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

logger = logging.getLogger("Kafka")
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
    client = KafkaAdminClient(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])
    # single topic create
    topic = NewTopic(name=os.environ['TOPICS_NAME'],
                     num_partitions=int(os.environ['TOPICS_PARTITIONS']),
                     replication_factor=int(os.environ['TOPICS_REPLICAS']),
                     topic_configs={"min.insync.replicas": 2}
                     )

    try:
        # topic already exist or not
        topics_list = client.list_topics()
        print("List of topics: ", topics_list)
        # if topic.name in topics_list:
        #     client.delete_topics([topic.name])
        #     print("After deleted, list of topics: ", client.list_topics())
        # """
        # Topic already exist: [Error 36] TopicAlreadyExistsError: Request 'CreateTopicsRequest_v3(create_topic_requests=[(topic='people.basic.python', num_partitions=3, replication_factor=3, replica_assignment=[], configs=[(config_key='min.insync.replicas', config_value=2)])], timeout=30000, validate_only=False)' failed with response 'CreateTopicsResponse_v3(throttle_time_ms=0, topic_errors=[(topic='people.basic.python', error_code=36, error_message="Topic 'people.basic.python' is marked for deletion.")])'.
        # """
        #if you delete topic
        # time.sleep(5)
        # create topic
        client.create_topics([topic])
        print("After created, list of topics: ", client.list_topics())
        configs = client.describe_configs(
            config_resources=[ConfigResource(ConfigResourceType.TOPIC, os.environ['TOPICS_NAME'])],
            include_synonyms=True)

        items = configs[0].to_object()
        # schema
        # SCHEMA = Schema(
        #     ('throttle_time_ms', Int32),
        #     ('resources', Array(
        #         ('error_code', Int16),
        #         ('error_message', String('utf-8')),
        #         ('resource_type', Int8),
        #         ('resource_name', String('utf-8')),
        #         ('config_entries', Array(
        #             ('config_names', String('utf-8')),
        #             ('config_value', String('utf-8')),
        #             ('read_only', Boolean),
        #             ('config_source', Int8),
        #             ('is_sensitive', Boolean),
        #             ('config_synonyms', Array(
        #                 ('config_name', String('utf-8')),
        #                 ('config_value', String('utf-8')),
        #                 ('config_source', Int8)))))))
        # )
        with open("default_config.json", "w") as file:
                file.write(json.dumps(dict(flatten(items)), indent=4))
        resource_update = ConfigResource(ConfigResourceType.TOPIC,
                                         os.environ['TOPICS_NAME'],
                                         configs={"retention.ms": '360000',
                                                  "min.insync.replicas": 2,
                                                  "partitions": int(os.environ['TOPICS_PARTITIONS'])
                                                  })
        client.alter_configs([resource_update])
        configs = client.describe_configs(
            config_resources=[ConfigResource(ConfigResourceType.TOPIC, os.environ['TOPICS_NAME'])],
        )
        items = configs[0].to_object()
        with open("updated_config.json", "w") as file:
            file.write(json.dumps(dict(flatten(items)), indent=4))
        print(configs)

    except TopicAlreadyExistsError as e:
        logger.warning("Topic already exist: " + str(e))
    finally:
        client.close()


@app.get("/hello-world")
async def hello_world():
    return {"messgae": "Hello world!!"}


def call_producer():
    return KafkaProducer(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])

@app.post("/api/people", status_code=201, response_model=List[Person])
async def create_person(cmd: CreatePeopleCommand):
    people: List[Person] = []

    faker = Faker()
    producer = call_producer()

    for _ in range(cmd.count):
        person = Person(id=str(uuid.uuid4()), name=faker.name(), title=faker.job().title())
        people.append(person)
        producer.send(topic=os.environ['TOPICS_NAME'],
                      key=person.title.lower().replace(r's+', '-').encode('utf-8'),
                      value=person.json().encode('utf-8')
                      )
    producer.flush()
    return people

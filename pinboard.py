from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Document, Keyword
import httpx
import typer
import hashlib
import json
import logging
import typing
import os

auth = os.environ.get("ELASTIC_PASSWORD")
token = os.environ.get("PINBOARD_TOKEN")
host = os.environ.get("ELASTICSEARCH_HOST", "localhost")
port = os.environ.get("ELASTICSEARCH_PORT", "9200")
client = Elasticsearch([f"elastic:{auth}@{host}:{port}"])


class Post(Document):
    tags = Keyword(multi=True)

    class Index:
        name = os.environ.get("ELASTIC_PINBOARD_INDEX", "pinboard")


methods = {"all": "posts/all", "recent": "posts/recent", "test": "posts/get"}


def transform_tags(tags:str):
    """convert the tags field to a list split on spaces"""
    return tags.split(" ")


def transform_meta(post_object: dict):
    """set the _id of the dict object to the objects meta"""
    return post_object['meta']


def transform(json_object: dict):

    for entry in json_object:
        entry['_id'] = transform_meta(entry)
        entry['tags'] = transform_tags(entry['tags'])
        yield entry

def pinboard_request(method: str, index: str = "pinboard"):

    if method not in methods.keys():
        msg = 'INVALID METHOD TYPE: Must be one of "all", "recent", "test"'
        raise ValueError(msg)

    url = f"https://api.pinboard.in/v1/{methods[method]}?auth_token={token}&format=json"
    logging.debug(f"{url=}")
    r = httpx.get(url, timeout=None)

    if r.status_code != 200:
        typer.echo(r.content)

    json_object = r.json() if method == "all" else r.json()["posts"]
    return bulk(client=client, index=index, actions=(x for x in transform(json_object)))


if __name__ == "__main__":
    Post.init(using=client)
    typer.run(pinboard_request)

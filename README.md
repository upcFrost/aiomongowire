# MongoDB Wire Protocol for asyncio

This is a [MongoDB Wire Protocol](https://docs.mongodb.com/manual/reference/mongodb-wire-protocol)
client implementation for Python [asyncio](https://docs.python.org/3/library/asyncio.html).

## How is it different from Motor

[Motor](https://github.com/mongodb/motor) is a full-featured MongoDB client for asyncio. Aiomongowire is not trying to
become a high-level client, it does not implement common command like `find()` or `updateOne()`, it is a pure minimal
protocol client which can be used as a building block to create a higher level client.

Motor uses `asyncio.run_in_executor` to make the synchronous code work with asyncio. Aiomongowire is based
on [asyncio.Protocol](https://docs.python.org/3/library/asyncio-protocol.html), which is an asyncio-native to
communicate over network.

Motor is based on Pymongo, and brings it as a dependency, which makes it much larger and monolithic. Aiomongowire tries
to avoid the scope creep, and is kept separate from BSON parsers, higher level logic, and syntactic sugar.

## Install

To get the current dev release, use test PyPi:

```
pip install -i https://test.pypi.org/simple/ aiomongowire
```

## Usage

Aiomongowire is made to be used as a building block inside a bigger MongoDB client, but it can be used separately if you
want to remove as many intermediate layer as you can between your code and the DB.

```python
import asyncio
from aiomongowire import OpQuery, OpInsert, MongoWireMessage, MongoWireProtocol

db_name = "test"
collection_name = "test"
value = 123

# Connect
loop = asyncio.get_event_loop()
transport, protocol = await loop.create_connection(lambda: MongoWireProtocol(), '127.0.0.1', 27017)

# Insert data
operation = OpInsert(full_collection_name=f"{db_name}.{collection_name}", 
                     documents=[{'a': value}])
data = MongoWireMessage(operation=operation)
await protocol.send_data(data)

# Query data
operation = OpQuery(full_collection_name=f"{db_name}.{collection_name}",
                    query={'a': value})
data = MongoWireMessage(operation=operation)
result: MongoWireMessage = await protocol.send_data(data)

# Disconnect
transport.close()
```
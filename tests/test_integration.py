import asyncio
import os
import uuid
from asyncio import Future
from time import sleep

import pytest

import src.aiomongowire as aiomongowire
from src.aiomongowire import MongoWireMessage
from src.aiomongowire.base_op import BaseOp
from src.aiomongowire.op_code import OpCode


@pytest.fixture
def db_name() -> str:
    return uuid.uuid4().hex[:10]


@pytest.fixture
def collection_name() -> str:
    return uuid.uuid4().hex[:10]


if os.environ.get('GITHUB_ENV'):
    @pytest.fixture(scope='session')
    def mongo():
        # Mongo takes time to init
        sleep(5)
else:
    @pytest.fixture(scope='session')
    def mongo(docker_db):
        # Mongo takes time to init
        sleep(5)


@pytest.fixture
async def protocol(mongo) -> aiomongowire.MongoWireProtocol:
    loop = asyncio.get_event_loop()
    transport, protocol = await loop.create_connection(lambda: aiomongowire.protocol.MongoWireProtocol(),
                                                       '127.0.0.1', 27017)
    yield protocol
    transport.close()


@pytest.mark.asyncio
async def test_connect(protocol):
    assert protocol.connected


@pytest.mark.asyncio
async def test_is_master(protocol):
    assert protocol.connected
    operation = aiomongowire.OpQuery(full_collection_name='admin.$cmd', query={'isMaster': 1}, number_to_return=1)
    data = MongoWireMessage(operation=operation)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    result: MongoWireMessage = await future

    assert isinstance(result.operation, aiomongowire.OpReply)
    assert len(result.operation.documents) == 1
    assert result.operation.documents[0]['ok'] == 1


@pytest.mark.asyncio
async def test_send_op_msg_insert(protocol, db_name, collection_name):
    assert protocol.connected
    header = aiomongowire.message_header.MessageHeader()
    operation = aiomongowire.OpMsg(sections=[aiomongowire.OpMsg.Insert(db=db_name, collection=collection_name),
                                             aiomongowire.OpMsg.Document(0, 'documents', [{'a': 1}])])
    data = MongoWireMessage(header=header, operation=operation)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    result = await future
    assert result.operation.op_code() == OpCode.OP_MSG
    assert result.header.response_to == header.request_id


@pytest.mark.asyncio
async def test_send_op_insert(protocol, db_name, collection_name):
    assert protocol.connected
    operation = aiomongowire.OpInsert(f"{db_name}.{collection_name}", [{'a': 1}])
    data = MongoWireMessage(operation=operation)
    future: Future[BaseOp] = await protocol.send_data(data)
    result = await future
    assert result is None


@pytest.mark.asyncio
async def test_send_op_query_no_such_db(protocol, db_name, collection_name):
    assert protocol.connected
    header = aiomongowire.MessageHeader()
    operation = aiomongowire.OpQuery(full_collection_name=f"{db_name}.{collection_name}", query={})
    data = MongoWireMessage(header=header, operation=operation)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    result = await future
    assert result.operation.op_code() == OpCode.OP_REPLY
    assert result.header.response_to == header.request_id


@pytest.mark.asyncio
async def test_insert_and_query(protocol, db_name, collection_name):
    assert protocol.connected
    object_id = uuid.uuid4().hex

    operation = aiomongowire.OpInsert(f"{db_name}.{collection_name}", [{'a': object_id}])
    data = MongoWireMessage(operation=operation)
    future: Future[BaseOp] = await protocol.send_data(data)
    await future

    operation = aiomongowire.OpQuery(full_collection_name=f"{db_name}.{collection_name}",
                                     query={'a': object_id}, return_fields_selector=None)
    data = MongoWireMessage(operation=operation)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    result: MongoWireMessage = await future

    assert isinstance(result.operation, aiomongowire.OpReply)
    assert result.operation.documents[0]['a'] == object_id


@pytest.mark.asyncio
async def test_insert_update_and_query(protocol, db_name, collection_name):
    assert protocol.connected
    object_id = uuid.uuid4().hex

    operation = aiomongowire.OpInsert(f"{db_name}.{collection_name}", [{'a': object_id}])
    data = MongoWireMessage(operation=operation)
    future: Future[BaseOp] = await protocol.send_data(data)
    await future

    new_object_id = uuid.uuid4().hex
    operation = aiomongowire.OpUpdate(full_collection_name=f"{db_name}.{collection_name}",
                                      selector={'a': object_id},
                                      update={'$set': {'a': new_object_id}})
    data = MongoWireMessage(operation=operation)
    future: Future[BaseOp] = await protocol.send_data(data)
    await future

    operation = aiomongowire.OpQuery(full_collection_name=f"{db_name}.{collection_name}",
                                     query={'a': new_object_id}, return_fields_selector=None)
    data = MongoWireMessage(operation=operation)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    result: MongoWireMessage = await future

    assert result.operation.documents[0]['a'] == new_object_id


@pytest.mark.asyncio
async def test_insert_delete_and_query(protocol, db_name, collection_name):
    assert protocol.connected
    object_id = uuid.uuid4().hex

    operation = aiomongowire.OpInsert(f"{db_name}.{collection_name}", [{'a': object_id}])
    data = MongoWireMessage(operation=operation)
    future: Future[BaseOp] = await protocol.send_data(data)
    await future

    operation = aiomongowire.OpDelete(full_collection_name=f"{db_name}.{collection_name}",
                                      selector={'a': object_id})
    data = MongoWireMessage(operation=operation)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    await future

    operation = aiomongowire.OpQuery(full_collection_name=f"{db_name}.{collection_name}",
                                     query={'a': object_id}, return_fields_selector=None)
    data = MongoWireMessage(operation=operation)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    result: MongoWireMessage = await future

    assert isinstance(result.operation, aiomongowire.OpReply)
    assert not result.operation.documents


@pytest.mark.asyncio
async def test_get_more_no_cursor(protocol, db_name, collection_name):
    object_id = uuid.uuid4().hex
    operation = aiomongowire.OpInsert(f"{db_name}.{collection_name}", [{'a': object_id}])
    data = MongoWireMessage(operation=operation)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    await future

    operation = aiomongowire.OpGetMore(full_collection_name=f"{db_name}.{collection_name}",
                                       number_to_return=1, cursor_id=123)
    data = MongoWireMessage(operation=operation)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    result: MongoWireMessage = await future

    assert isinstance(result.operation, aiomongowire.OpReply)
    assert result.operation.response_flags == aiomongowire.OpReply.Flags.CURSOR_NOT_FOUND


@pytest.mark.asyncio
async def test_insert_and_query_has_more(protocol, db_name, collection_name):
    assert protocol.connected

    record_num = 20
    to_return_num = 10

    for i in range(record_num):
        object_id = uuid.uuid4().hex
        operation = aiomongowire.OpInsert(f"{db_name}.{collection_name}", [{'a': object_id}])
        data = MongoWireMessage(operation=operation)
        future: Future[MongoWireMessage] = await protocol.send_data(data)
        await future

    operation = aiomongowire.OpQuery(full_collection_name=f"{db_name}.{collection_name}", query={},
                                     number_to_return=to_return_num)
    data = MongoWireMessage(operation=operation)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    result: MongoWireMessage = await future

    assert isinstance(result.operation, aiomongowire.OpReply)
    assert len(result.operation.documents) == to_return_num
    assert result.operation.cursor_id

    operation = aiomongowire.OpGetMore(full_collection_name=f"{db_name}.{collection_name}",
                                       number_to_return=to_return_num, cursor_id=result.operation.cursor_id)
    data = MongoWireMessage(operation=operation)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    result: MongoWireMessage = await future

    assert isinstance(result.operation, aiomongowire.OpReply)
    assert len(result.operation.documents) == to_return_num
    assert result.operation.cursor_id


@pytest.mark.parametrize('compressor', [
    aiomongowire.OpCompressed.Compressor.SNAPPY,
    aiomongowire.OpCompressed.Compressor.ZLIB,
    aiomongowire.OpCompressed.Compressor.ZSTD
])
@pytest.mark.asyncio
async def test_compressed(protocol, db_name, collection_name, compressor):
    query = aiomongowire.OpQuery(full_collection_name=f"{db_name}.{collection_name}", query={})
    compressed = aiomongowire.OpCompressed(compressor=compressor, original_msg=query)
    data = MongoWireMessage(operation=compressed)
    future: Future[MongoWireMessage] = await protocol.send_data(data)
    result = await future
    assert isinstance(result.operation, aiomongowire.OpCompressed)
    assert isinstance(result.operation.original_msg, aiomongowire.OpReply)

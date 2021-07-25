import asyncio
import os
import uuid
from asyncio import Future
from time import sleep

import pytest

import aiomongowire
from aiomongowire.base_op import BaseOp
from aiomongowire.op_code import OpCode


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
async def test_send_op_msg_insert(protocol, db_name, collection_name):
    assert protocol.connected
    header = aiomongowire.message_header.MessageHeader()
    data = aiomongowire.OpMsg(header=header,
                              sections=[aiomongowire.OpMsg.Insert(db=db_name, collection=collection_name),
                                        aiomongowire.OpMsg.Document(0, 'documents', [{'a': 1}])])
    future: Future[BaseOp] = await protocol.send_data(data)
    result = await future
    assert result.op_code() == OpCode.OP_MSG
    assert result.header.response_to == header.request_id


@pytest.mark.asyncio
async def test_send_op_insert(protocol, db_name, collection_name):
    assert protocol.connected
    data = aiomongowire.OpInsert(f"{db_name}.{collection_name}", [{'a': 1}])
    future: Future[BaseOp] = await protocol.send_data(data)
    result = await future
    assert result is None


@pytest.mark.asyncio
async def test_send_op_query_no_such_db(protocol, db_name, collection_name):
    assert protocol.connected
    header = aiomongowire.MessageHeader()
    data = aiomongowire.OpQuery(header=header, full_collection_name=f"{db_name}.{collection_name}", query={})
    future: Future[BaseOp] = await protocol.send_data(data)
    result = await future
    assert result.op_code() == OpCode.OP_REPLY
    assert result.header.response_to == header.request_id


@pytest.mark.asyncio
async def test_insert_and_query(protocol, db_name, collection_name):
    assert protocol.connected
    object_id = uuid.uuid4().hex

    data = aiomongowire.OpInsert(f"{db_name}.{collection_name}", [{'a': object_id}])
    future: Future[BaseOp] = await protocol.send_data(data)
    await future

    data = aiomongowire.OpQuery(full_collection_name=f"{db_name}.{collection_name}",
                                query={'a': object_id}, return_fields_selector=None)
    future: Future[aiomongowire.OpReply] = await protocol.send_data(data)
    result: aiomongowire.OpReply = await future

    assert result.documents[0]['a'] == object_id


@pytest.mark.asyncio
async def test_get_more_no_cursor(protocol, db_name, collection_name):
    object_id = uuid.uuid4().hex
    data = aiomongowire.OpInsert(f"{db_name}.{collection_name}", [{'a': object_id}])
    future: Future[BaseOp] = await protocol.send_data(data)
    await future

    data = aiomongowire.OpGetMore(full_collection_name=f"{db_name}.{collection_name}",
                                  number_to_return=1, cursor_id=123)
    future: Future[aiomongowire.OpReply] = await protocol.send_data(data)
    result: aiomongowire.OpReply = await future

    assert isinstance(result, aiomongowire.OpReply)
    assert result.response_flags == aiomongowire.OpReply.Flags.CURSOR_NOT_FOUND


@pytest.mark.asyncio
async def test_insert_and_query_has_more(protocol, db_name, collection_name):
    assert protocol.connected

    record_num = 20
    to_return_num = 10

    for i in range(record_num):
        object_id = uuid.uuid4().hex
        data = aiomongowire.OpInsert(f"{db_name}.{collection_name}", [{'a': object_id}])
        future: Future[BaseOp] = await protocol.send_data(data)
        await future

    data = aiomongowire.OpQuery(full_collection_name=f"{db_name}.{collection_name}", query={},
                                number_to_return=to_return_num)
    future: Future[aiomongowire.OpReply] = await protocol.send_data(data)
    result: aiomongowire.OpReply = await future

    assert isinstance(result, aiomongowire.OpReply)
    assert len(result.documents) == to_return_num
    assert result.cursor_id

    data = aiomongowire.OpGetMore(full_collection_name=f"{db_name}.{collection_name}",
                                  number_to_return=to_return_num, cursor_id=result.cursor_id)
    future: Future[aiomongowire.OpReply] = await protocol.send_data(data)
    result: aiomongowire.OpReply = await future

    assert isinstance(result, aiomongowire.OpReply)
    assert len(result.documents) == to_return_num
    assert result.cursor_id

import asyncio
import os
import uuid
from asyncio import Future
from random import randint
from time import sleep

import pytest
import pytest_docker_db.plugin

import aiomongowire
import aiomongowire.message_header
import aiomongowire.op_code
import aiomongowire.op_msg
import aiomongowire.op_query
import aiomongowire.op_reply
import aiomongowire.protocol
from aiomongowire import OpReply
from aiomongowire.base_op import BaseOp
from aiomongowire.op_code import OpCode


@pytest.fixture
def request_id() -> int:
    return randint(10, 10000)


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
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_connection(lambda: aiomongowire.protocol.MongoWireProtocol(),
                                                       '127.0.0.1', 27017)
    yield protocol
    transport.close()


@pytest.mark.asyncio
async def test_connect(protocol):
    assert protocol.connected


@pytest.mark.asyncio
async def test_send_op_msg_insert(request_id, protocol, db_name, collection_name):
    assert protocol.connected
    header = aiomongowire.message_header.MessageHeader(request_id=request_id, response_to=0)
    data = aiomongowire.op_msg.OpMsg(header, 0,
                                     [aiomongowire.op_msg.OpMsg.Insert(db=db_name, collection=collection_name),
                                      aiomongowire.op_msg.OpMsg.Document(0, 'documents', [{'a': 1}])])
    future: Future[BaseOp] = await protocol.send_data(data)
    result = await future
    assert result.op_code() == OpCode.OP_MSG
    assert result.header.response_to == request_id


@pytest.mark.asyncio
async def test_send_op_insert(request_id, protocol, db_name, collection_name):
    assert protocol.connected
    header = aiomongowire.message_header.MessageHeader(request_id=request_id, response_to=0)
    data = aiomongowire.op_insert.OpInsert(header, 0, f"{db_name}.{collection_name}", [{'a': 1}])
    future: Future[BaseOp] = await protocol.send_data(data)
    result = await future
    assert result is None


@pytest.mark.asyncio
async def test_send_op_query_no_such_db(request_id, protocol, db_name, collection_name):
    assert protocol.connected
    header = aiomongowire.message_header.MessageHeader(request_id=request_id, response_to=0)
    data = aiomongowire.op_query.OpQuery(header=header,
                                         flags=0, full_collection_name=f"{db_name}.{collection_name}",
                                         number_to_skip=0, number_to_return=10,
                                         query={}, return_fields_selector=None)
    future: Future[BaseOp] = await protocol.send_data(data)
    result = await future
    assert result.op_code() == OpCode.OP_REPLY
    assert result.header.response_to == request_id


@pytest.mark.asyncio
async def test_insert_and_query(request_id, protocol, db_name, collection_name):
    assert protocol.connected
    header = aiomongowire.message_header.MessageHeader(request_id=request_id, response_to=0)
    object_id = uuid.uuid4().hex

    data = aiomongowire.op_insert.OpInsert(header, 0, f"{db_name}.{collection_name}", [{'a': object_id}])
    future: Future[BaseOp] = await protocol.send_data(data)
    await future

    header = aiomongowire.message_header.MessageHeader(request_id=request_id + 1, response_to=0)
    data = aiomongowire.op_query.OpQuery(header=header,
                                         flags=0, full_collection_name=f"{db_name}.{collection_name}",
                                         number_to_skip=0, number_to_return=10,
                                         query={'a': object_id}, return_fields_selector=None)
    future: Future[OpReply] = await protocol.send_data(data)
    result: OpReply = await future

    assert result.documents[0]['a'] == object_id


@pytest.mark.asyncio
async def test_get_more_no_cursor(request_id, protocol, db_name, collection_name):
    header = aiomongowire.message_header.MessageHeader(request_id=request_id, response_to=0)
    object_id = uuid.uuid4().hex
    data = aiomongowire.op_insert.OpInsert(header, 0, f"{db_name}.{collection_name}", [{'a': object_id}])
    future: Future[BaseOp] = await protocol.send_data(data)
    await future

    header = aiomongowire.message_header.MessageHeader(request_id=request_id + 1, response_to=0)
    data = aiomongowire.op_get_more.OpGetMore(
        header=header, full_collection_name=f"{db_name}.{collection_name}",
        number_to_return=1, cursor_id=123)
    future: Future[OpReply] = await protocol.send_data(data)
    result: OpReply = await future

    assert isinstance(result, OpReply)
    assert result.response_flags == OpReply.Flags.QUERY_FAILURE


@pytest.mark.asyncio
async def test_insert_and_query_has_more(request_id, protocol, db_name, collection_name):
    assert protocol.connected
    header = aiomongowire.message_header.MessageHeader(request_id=request_id, response_to=0)

    record_num = 20
    to_return_num = 10

    for i in range(record_num):
        object_id = uuid.uuid4().hex
        data = aiomongowire.op_insert.OpInsert(header, 0, f"{db_name}.{collection_name}", [{'a': object_id}])
        future: Future[BaseOp] = await protocol.send_data(data)
        await future

    header = aiomongowire.message_header.MessageHeader(request_id=request_id + 1, response_to=0)
    data = aiomongowire.op_query.OpQuery(header=header,
                                         flags=0, full_collection_name=f"{db_name}.{collection_name}",
                                         number_to_skip=0, number_to_return=to_return_num,
                                         query={}, return_fields_selector=None)
    future: Future[OpReply] = await protocol.send_data(data)
    result: OpReply = await future

    assert isinstance(result, OpReply)
    assert len(result.documents) == to_return_num
    assert result.cursor_id

    header = aiomongowire.message_header.MessageHeader(request_id=request_id + 1, response_to=0)
    data = aiomongowire.op_get_more.OpGetMore(
        header=header, full_collection_name=f"{db_name}.{collection_name}",
        number_to_return=to_return_num, cursor_id=result.cursor_id)
    future: Future[OpReply] = await protocol.send_data(data)
    result: OpReply = await future

    assert isinstance(result, OpReply)
    assert len(result.documents) == to_return_num
    assert result.cursor_id

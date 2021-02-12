import asyncio
from asyncio import Future
from random import randint

import pytest
from bson import ObjectId

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
async def protocol() -> aiomongowire.MongoWireProtocol:
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_connection(lambda: aiomongowire.protocol.MongoWireProtocol(),
                                                       '127.0.0.1', 27017)
    yield protocol
    transport.close()


@pytest.mark.asyncio
async def test_connect(protocol):
    assert protocol.connected


@pytest.mark.asyncio
async def test_send_op_msg_insert(request_id, protocol):
    header = aiomongowire.message_header.MessageHeader(request_id=request_id, response_to=0)
    data = aiomongowire.op_msg.OpMsg(header, 0, [aiomongowire.op_msg.OpMsg.Insert(),
                                                 aiomongowire.op_msg.OpMsg.Document(0, 'documents', [{'a': 1}])])
    future: Future[BaseOp] = await protocol.send_data(data)
    result = await future
    assert result.op_code() == OpCode.OP_MSG
    assert result.header.response_to == request_id


@pytest.mark.asyncio
async def test_send_op_insert(request_id, protocol):
    header = aiomongowire.message_header.MessageHeader(request_id=request_id, response_to=0)
    data = aiomongowire.op_insert.OpInsert(header, 0, "test.collection", [{'a': 1}])
    future: Future[BaseOp] = await protocol.send_data(data)
    result = await future
    assert result is None


@pytest.mark.asyncio
async def test_send_op_query(request_id, protocol):
    header = aiomongowire.message_header.MessageHeader(request_id=request_id, response_to=0)
    data = aiomongowire.op_query.OpQuery(header=header,
                                         flags=0, full_collection_name='test.collection',
                                         number_to_skip=0, number_to_return=10,
                                         query={}, return_fields_selector=None)
    future: Future[BaseOp] = await protocol.send_data(data)
    result = await future
    assert result.op_code() == OpCode.OP_REPLY
    assert result.header.response_to == request_id


@pytest.mark.asyncio
async def test_insert_and_query(request_id, protocol):
    header = aiomongowire.message_header.MessageHeader(request_id=request_id, response_to=0)
    object_id = str(ObjectId())

    data = aiomongowire.op_insert.OpInsert(header, 0, "test.collection", [{'a': object_id}])
    future: Future[BaseOp] = await protocol.send_data(data)
    await future

    header = aiomongowire.message_header.MessageHeader(request_id=request_id + 1, response_to=0)
    data = aiomongowire.op_query.OpQuery(header=header,
                                         flags=0, full_collection_name='test.collection',
                                         number_to_skip=0, number_to_return=10,
                                         query={'a': object_id}, return_fields_selector=None)
    future: Future[OpReply] = await protocol.send_data(data)
    result: OpReply = await future

    assert result.documents[0]['a'] == object_id

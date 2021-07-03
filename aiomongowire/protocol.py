import asyncio
import io
import logging
import traceback
from asyncio import transports, Future
from typing import Optional, Dict

from aiomongowire.base_op import BaseOp


class MongoWireProtocol(asyncio.Protocol):
    def __init__(self):
        self.connected: bool = False

        self._transport: Optional[asyncio.Transport] = None
        self._msg_queue: asyncio.Queue[Optional[BaseOp]] = asyncio.Queue()
        self._out_data: Dict[int, Future[BaseOp]] = dict()
        self._logger = logging.getLogger('aiomongowire')

    async def send_data(self, data: BaseOp) -> Future:
        """
        Adds data to the sending queue and returns future.
        If the OP is not supposed to return anything, future is returned completed with None inside

        :param data: Data to send
        :return: Response future
        """
        future = Future()
        if data.has_reply():
            self._out_data[data.header.request_id] = future
        else:
            future.set_result(None)
        await self._msg_queue.put(data)
        return future

    async def _send_loop(self):
        """
        Data sending loop
        """
        while self.connected:
            try:
                data = await self._msg_queue.get()
            except asyncio.CancelledError:
                self._logger.info("AioMongoWire exiting")
                break

            if not data:
                # Data might be None when exiting
                continue

            try:
                self._transport.write(bytes(data))
            except Exception as exc:
                self._logger.error(traceback.format_exc())
                self._out_data[data.header.request_id].set_exception(exc)
            finally:
                self._msg_queue.task_done()

    def data_received(self, data: bytes):
        """
        Decodes received data and tries to map it to the request future
        """
        try:
            with io.BytesIO(data) as recv:
                msg = BaseOp.from_data(recv)
        except Exception:
            self._logger.error(traceback.format_exc())
            return

        try:
            self._out_data[msg.header.response_to].set_result(msg)
        except KeyError:
            self._logger.error(f"Unexpected response to non-existent request {msg.header.response_to}")

    def eof_received(self) -> Optional[bool]:
        return super().eof_received()

    def connection_made(self, transport: transports.BaseTransport) -> None:
        self._transport = transport
        self.connected = True
        asyncio.get_event_loop().create_task(self._send_loop())

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.connected = False
        await self._msg_queue.put(None)
        await self._msg_queue.join()
        if exc:
            raise exc

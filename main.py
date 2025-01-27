import asyncio
from tornado import gen
from collections import deque
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError, IOStream
import logging
from tornado.ioloop import IOLoop
from datetime import datetime
import sys
from redis.asyncio import from_url


def setup_logger(name, log_file, level=logging.DEBUG):
    handler = logging.FileHandler(f"/root/tracking/{log_file}")
    formatter = logging.Formatter('%(asctime)s => %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


class EchoServer(TCPServer):
    def __init__(self):
        super().__init__()

    @staticmethod
    async def _remove_stream(stream, imei_: str | None):
        global active_streams, defined_streams
        await redis_instance.publish('status', f'REMOVED {imei_}')

        if stream in active_streams:
            active_streams.remove(stream)

        defined_streams[imei_] = None

    @staticmethod
    async def _define_stream(stream, imei_: str):
        # $,E,EGAS,2.2.02,NR,02,H,866477068932006,NA00000000,1,01042024,163731,15.109019,N,076.897030,E,26.8,177.3,42,469,0.7,0.4,BSNL MOBIL,1,1,27.7,2.55,0,O,17,
        # 404,71,9DE,66F6,66F7,9DE,13,66D9,9DE,9,66F5,9DE,8,6611,9DE,5,0001,00,027693,1658.5,00,*
        global defined_streams

        if imei_:
            await redis_instance.set(imei_, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            defined_streams[imei_] = stream

    @staticmethod
    def _get_imei(data: str) -> str:
        imei = ''
        res = data.split(',')
        if len(res) > 18:
            if data.startswith('$,E,EGAS'):
                imei = res[7]
            elif data.startswith('$PVT,EGAS'):
                imei = res[6]
        return imei

    @gen.coroutine
    async def handle_stream(self, stream: IOStream, address):
        global active_streams
        active_streams.append(stream)

        async def handle_connection():
            imei = None
            flag = False
            while True:
                try:
                    data = await stream.read_until(b"\n")
                    data = data.decode()
                    imei = imei or self._get_imei(data)

                    if not flag:
                        await redis_instance.publish('status', f"CONNECTED {imei}")
                        flag = True
                    asyncio.create_task(self._define_stream(stream, imei))

                except asyncio.TimeoutError as e:
                    error_logger.info(f"{e} -> {imei}")
                    asyncio.create_task(self._remove_stream(stream, imei))
                    break

                except StreamClosedError:
                    asyncio.create_task(self._remove_stream(stream, imei))
                    break

                except Exception as e:
                    error_logger.info(f"ERROR: {e}")
                    if stream.closed():
                        asyncio.create_task(self._remove_stream(stream, imei))
                        break

        await asyncio.create_task(handle_connection())


async def send_data_to_device(imei: str, command: str):
    error_logger.info(f'Command received->{imei}, {command}')
    flag = False
    while True:
        if imei in defined_streams:
            error_logger.info(f"{imei} found in {val}.")
            stream = defined_streams[imei]
            if stream:
                for i in range(5):
                    if not stream.closed():
                        await stream.write(command.encode())
                        flag = True
                        error_logger.info(f"written to {imei} from {val}.")
                        await asyncio.sleep(10)
                    else:
                        if flag:
                            break
                if flag:
                    break

            else:
                error_logger.info(f"stream of {imei} closed from {val}...sleeping 10 sec.")
                await asyncio.sleep(10)

        else:
            await asyncio.sleep(10)


async def consume_msgs():
    command_pubsub = redis_instance.pubsub()
    await command_pubsub.subscribe("commands")

    async for message in command_pubsub.listen():
        if message["type"] == "message":
            data = message["data"]
            error_logger.info(data)
            if data:
                imei, command = data.split('$&$')
                asyncio.create_task(send_data_to_device(imei, command))


async def flush_redis():
    if val == '1':
        await redis_instance.flushall()
        error_logger.info(f'>>>>>>>>>>>>Redis flushed.<<<<<<<<<<<<<')


# reading from command line argument
val = 1
for i in sys.argv:
    ls = i.split('=')
    for j in ls:
        if j == '-srn':
            val = ls[1]
        else:
            break


error_logger = setup_logger('error_logger', 'error.log')

active_streams = deque()
defined_streams: dict[str, IOStream | None] = {}

REDIS_URL = "redis://redis:6379"
redis_instance = from_url(REDIS_URL, decode_responses=True)

server = EchoServer()
server.listen(32222, reuse_port=True, backlog=65535)

IOLoop.current().spawn_callback(consume_msgs)
IOLoop.current().spawn_callback(flush_redis)
IOLoop.current().start()

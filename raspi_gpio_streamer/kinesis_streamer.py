from abc import ABCMeta, abstractmethod
import asyncio
import os
import sys
import re
import threading
import time
from queue import Queue, Empty
from typing import List, Optional

import boto3 as boto3
from raspi_gpio_streamer.log import logger


KINESIS_MSG_MAX_BYTES = 1000000
SEND_INTERVAL_SEC = 1


# class SendLog:
#     key_re = re.compile(r".*\$\$\$")

#     def __init__(self, url, channel):
#         self._url = url
#         self._channel = channel
#         self._thread = None
#         self._loop = asyncio.new_event_loop()
#         self._queue = None

#     def publish(self, data):
#         if self._queue:
#             self._loop.call_soon_threadsafe(self._queue.put_nowait, data)
#         else:
#             logger.error("send log queue not found")

#     def start(self):
#         self._thread = threading.Thread(target=self.run, args=(self._loop,), daemon=True).start()

#     def run(self, loop):
#         asyncio.set_event_loop(loop)
#         loop.run_until_complete(self.send())

#     async def send(self):
#         self._queue = asyncio.Queue()
#         redis = await aioredis.create_redis_pool(self._url)
#         while True:
#             data = await self._queue.get()
#             # for line in [l for l in re.sub(".*\$\$\$", "", data).split("\n") if l]:
#             #    await redis.publish(self._channel, line)
#             clean = self.key_re.sub("", data)
#             await redis.publish(self._channel, clean)

class KinesisStreamer():
    key_re = re.compile(r".*\$\$\$")

    def __init__(self, stream_name: str):
        self.__kinesis = boto3.client("kinesis")
        # self.__kinesis.describe_limits()  # just check network connection
        self.__stream_name = stream_name
        self.__send_q = Queue()
        self.__should_worker_run = False
        self.__is_running = False
        # self.__fragmented_msg = ""
        self._push_back = None
        self._current_chunk = None
        # redis_url = os.environ.get("UNAAS_REDIS_URL", "redis://localhost")
        # channel = os.environ.get("UNAAS_REPEATER_LOG_CHANNEL", "unaas_repeater_log")
        self.__send_msg = None
        # self.__send_log = SendLog(redis_url, channel)
        self._error = False

    def has_error(self):
        return self._error

    def enq(self, msg_list: List):
        self.__send_q.put(msg_list)

    def start(self):
        if self.__is_running:
            logger.warning("{}: Already Started".format(self.__class__.__name__))
            return False
        self.__should_worker_run = True
        threading.Thread(target=self.__worker, daemon=True).start()
        # self.__send_log.start()
        logger.info("{}: Started".format(self.__class__.__name__))
        return True

    def stop(self):
        if not self.__is_running:
            logger.warning("{}: Already Stopped".format(self.__class__.__name__))
            return False
        self.__should_worker_run = False
        while self.__is_running:
            time.sleep(1)
        logger.info("{}: Stopped".format(self.__class__.__name__))
        return True

    def __worker(self):
        self._error = False
        self.__is_running = True
        self.__send_msg = None
        try:
            while self.__should_worker_run:
                time.sleep(SEND_INTERVAL_SEC)
                self.__send_msg = self._build_message()
                if self.__send_msg == "":
                    logger.debug("Send Message Skipped: No data.")
                    self.__send_msg = None
                    continue

                timestamp_time = int(time.time() * 1000)
                self.__kinesis.put_record(
                    StreamName=self.__stream_name,
                    PartitionKey=str(timestamp_time),
                    Data=self.__send_msg.encode("UTF-8"),
                )
                # self.__send_log.publish(self.__send_msg)
                self.__send_msg = None
        except Exception:
            self._error = True
            self.__should_worker_run = False

        self.__is_running = False

    def _build_message(self):
        payload = ""
        envelope_len = len(self._get_send_message(""))
        max_length = KINESIS_MSG_MAX_BYTES - envelope_len

        while True:
            next_line = self.get_next_line()
            if not next_line:
                break
            elif len(payload) + len(next_line) + 1 > max_length:
                self.push_back_line(next_line)
                break
            else:
                payload = payload + next_line + "\n"

        payload.rstrip()
        if payload:
            return self._get_send_message(payload)
        return ""

    def get_next_line(self):
        line = None
        if self._push_back:
            line = self._push_back
            self._push_back = None
        elif type(self._current_chunk) is list and len(self._current_chunk) > 0:
            line = self._current_chunk.pop(0)
        else:
            while True:
                try:
                    ch = self.__send_q.get_nowait()
                except Empty:
                    break
                chunk = [x for x in ch if x]
                if len(chunk) > 0:
                    self._current_chunk = chunk
                    line = self._current_chunk.pop(0)
                    break
        return line

    def push_back_line(self, line):
        self._push_back = line

    def _get_send_message(self, msg: str) -> str:
        return msg

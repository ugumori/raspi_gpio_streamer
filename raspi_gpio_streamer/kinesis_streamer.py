import re
import threading
import time
from queue import Queue
from typing import List, Union
import boto3 as boto3

from raspi_gpio_streamer.log import logger
from raspi_gpio_streamer.config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_ACCESS_KEY,
    AWS_DEFAULT_REGION,
    HW_ID,
)

SEND_INTERVAL_SEC = 0.1


class KinesisStreamer:
    key_re = re.compile(r".*\$\$\$")

    def __init__(self, stream_name: str):
        self.__kinesis = boto3.client(
            "kinesis",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_DEFAULT_REGION,
        )
        self.__stream_name = stream_name
        self.__send_q = Queue()
        self.__is_running = False
        self.__worker_thread = None

    def enq(self, msg: Union[List, str]):
        msg_list = [msg] if type(msg) == str else msg
        for m in msg_list:
            self.__send_q.put(m)

    def start(self):
        if self.__is_running:
            logger.warning("Already Started")
            return False
        self.__is_running = True
        self.__worker_thread = threading.Thread(
            target=self.__worker, daemon=True
        ).start()
        logger.info("Started")
        return True

    def stop(self):
        if not self.__is_running:
            logger.warning("Already Stopped")
            return False
        if self.__worker_thread:
            self.__worker_thread.join()
        logger.info("Stopped")
        return True

    def __worker(self):
        try:
            while self.__is_running:
                time.sleep(SEND_INTERVAL_SEC)
                send_msg = self._build_message()
                if send_msg == "":
                    logger.debug("Send Message Skipped: No data.")
                    continue

                timestamp = int(time.time() * 1000)
                self.__kinesis.put_record(
                    StreamName=self.__stream_name,
                    PartitionKey=str(timestamp),
                    Data=send_msg.encode("UTF-8"),
                )
        except Exception as e:
            import traceback

            logger.error(e)
            logger.error(traceback.format_exc())

        self.__is_running = False

    def _build_message(self):
        next_line = self.__send_q.get()
        payload = HW_ID + "," + next_line + "\n"
        payload.rstrip()

        return payload

import sys
from raspi_gpio_streamer.kinesis_streamer import KinesisStreamer
from raspi_gpio_streamer.gpio_reader import RaspiGPIO, RaspiGPIO_IF
from raspi_gpio_streamer.log import logger

def main():
    class Connector(RaspiGPIO_IF):
        def __init__(self, streamer: KinesisStreamer):
            self._streamer = streamer

        def on_read_value(self, value, pin):
            logger.debug(f"{pin} {value}")
            self._streamer.enq([f"{pin},{value}"])

    streamer = KinesisStreamer("raspi_stream")
    connector = Connector(streamer)
    gpio_reader = RaspiGPIO()
    gpio_reader.set_callback(connector)

    streamer.start()
    gpio_reader.start()

    while True:
        pass


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover

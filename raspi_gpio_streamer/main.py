import sys
from raspi_gpio_streamer.kinesis_streamer import KinesisStreamer
from raspi_gpio_streamer.gpio_reader import RaspiGPIO, RaspiGPIO_IF


def main():
    class Connector(RaspiGPIO_IF):
        def __init__(self, streamer: KinesisStreamer):
            self._streamer = streamer

        def on_read_value(self, value, pin):
            self._streamer.enq([f"{pin} {value}"])

    streamer = KinesisStreamer("")
    connector = Connector(streamer)
    gpio_reader = RaspiGPIO()
    gpio_reader.set_callback(connector)

    streamer.start()
    gpio_reader.start()


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover

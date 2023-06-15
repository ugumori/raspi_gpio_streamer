from abc import abstractmethod
import RPi.GPIO as GPIO
import time
import threading
from raspi_gpio_streamer.log import logger
from abc import ABCMeta


GPIO_SB412A_1 = 40

class RaspiGPIO_IF(metaclass=ABCMeta):
    @abstractmethod
    def on_read_value(self, value, pin):
        pass


class RaspiGPIO:
    def __init__(self):
        self._callback = None
        self._thread = None
        self.is_stop = True
        GPIO.setmode(GPIO.BOARD)
        GPIO.setup(GPIO_SB412A_1, GPIO.IN, pull_up_down=GPIO.PUD_UP)

    def read(self):
        value = GPIO.input(GPIO_SB412A_1)
        return value

    def write(self):
        logger.warning("write() is not implemented")

    def set_callback(self, callback: RaspiGPIO_IF):
        self._callback = callback

    def start(self):
        self.is_stop = False
        self._thread = threading.Thread(target=self.__process, daemon=True).start()

    def stop(self):
        self.is_stop = True
        if self._thread:
            self._thread.join()
        GPIO.cleanup()

    def __process(self):
        while True:
            if self.is_stop:
                break
            value = self.read()
            if self._callback:
                self._callback.on_read_value(value, GPIO_SB412A_1)
            time.sleep(1)


class SampleIF(RaspiGPIO_IF):
    def on_read_value(self, value, pin):
        print(f"{pin} {value}")

if __name__ == "__main__":
    gpio = RaspiGPIO()
    # while True:
    #     value = gpio.read()
    #     print(value)
    #     time.sleep(1)
    gpio.set_callback(SampleIF())
    gpio.start()
    time.sleep(5)
    gpio.stop()

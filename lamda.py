class DataChangeDetector:
    def trigger_callback(self, trigger, callback):
        if trigger:
            callback(1, 2)


language = 'Python'
pto = language[0:6:3] #
print(pto) # Pto

class ForStub:
    def __init__(self, detector = DataChangeDetector()):
        self.__val = 1
        self.__detector = detector

    def func1(self, param1: int, param2: int) -> int:
        return param1 + param2

    def func1_name(self, *, param1: int, param2: int) -> int:
        return self.func1(param1, param2)

    def func2(self) -> None:
        return

    def func3(self) -> int:
        return self.__val

    def __private_func(self) -> str:
        return "private string value"

    def func4(self) -> str:
        return self.__private_func()

    def func5(self, trigger, callback) -> None:
        self.__detector.trigger_callback(trigger,callback)
from pytest_mockito import when
from test import lamda

def test1_func1(when):
    instance = lamda.ForStub()
    when(instance).func1(1, 1).thenReturn(3)

    assert 3 == instance.func1(1, 1)

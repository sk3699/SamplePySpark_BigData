from test import SessionIDGenerator
from pyspark.sql.types import Row
import datetime
from pytest_mock import MockerFixture

data = [Row(timestamp=datetime.datetime(2018, 1, 1, 3, 0), userid='u1', UnixTime=1514804400, diff=1801), Row(timestamp=datetime.datetime(2018, 1, 1, 4, 0), userid='u1', UnixTime=1514808000, diff=3600), Row(timestamp=datetime.datetime(2018, 1, 1, 4, 15), userid='u1', UnixTime=1514808900, diff=900)]


def test_fasak(mocker: MockerFixture) -> None:

    with mocker.patch(
        "test.SessionIDGenerator.generate_sessionIDs",
        return_value=['u1', datetime.datetime(2018, 1, 1, 3, 0), 'u1DE5b8O5nA4']
    ):
        actual_result = SessionIDGenerator.generate_sessionIDs(data)
        expected_result = ['u1', datetime.datetime(2018, 1, 1, 3, 0), 'u1DE5b8O5nA4']
        assert actual_result == expected_result


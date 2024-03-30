
import pytest
from unittest.mock import MagicMock, patch
from ..waer_time_util import waer_time_util

def test_waer_util_make_nanos_datetime_tz():

    input = "2024-03-21T20:23:20.000000+01:00"
    expected = 1711049000000000000

    print(f"INCOMING: {input}")
    result = waer_time_util.make_nanos(input)
    print(f"OUTGOING: {result}")

    assert result == expected


def test_waer_util_make_nanos_datetime():

    input = "2024-03-21T20:23:20.000000"
    expected = 1711049000000000000

    print(f"INCOMING: {input}")
    result = waer_time_util.make_nanos(input)
    print(f"OUTGOING: {result}")

    assert result == expected


def test_waer_util_make_nanos_date_only():

    input = "2024-03-21"
    expected = 1710975600000000000

    print(f"INCOMING: {input}")
    result = waer_time_util.make_nanos(input)
    print(f"OUTGOING: {result}")

    assert result == expected
import pytest


def test_increment():
    assert 4 == 4


# This test is designed to fail for demonstration purposes.
# def test_decrement():
#     assert 3 == 4


@pytest.mark.asyncio
async def test_pytest_asyncio():
    assert True

import polars as pl
import pytest
from polars_advanced_pivot import _apply_pivot, advanced_pivot

df = pl.DataFrame(
    {
        "A": [1, 2, 3, 4, 5],
        "fruits": ["banana", "banana", "apple", "apple", "banana"],
        "B": [5, 4, 3, 2, 1],
        "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
    }
)


@pytest.mark.asyncio
async def test_apply_pivot():
    df_pivoted = pl.DataFrame(
        {
            # "A": [1, 2, 3, 4, 5],
            "cars": ["beetle", "audi"],
            "banana": [2, 1],
            "apple": [2, None],
            # "B": [5, 4, 3, 2, 1],
        }
    )
    df_pivoted = df_pivoted.to_dicts()

    result = await _apply_pivot(
        df, index="cars", columns="fruits", value="A", aggregation_function="count"
    )
    result = result.to_dicts()

    assert True
    assert df_pivoted is not None
    assert result == df_pivoted


@pytest.mark.asyncio
async def test_apply_pivot_multiple_columns():
    df_pivoted = pl.DataFrame(
        {
            # "A": [1, 2, 3, 4, 5],
            "cars": ["beetle", "audi"],
            "banana_beetle": [2, None],
            "banana_audi": [None, 1],
            "apple_beetle": [2, None]
            # "B": [5, 4, 3, 2, 1],
        }
    )
    df_pivoted = df_pivoted.to_dicts()

    result = await _apply_pivot(
        df,
        index="cars",
        columns=["fruits", "cars"],
        value="A",
        aggregation_function="count",
    )
    result = result.to_dicts()

    assert True
    assert df_pivoted is not None
    assert result == df_pivoted


@pytest.mark.asyncio
async def test_apply_pivot_multiple_columns_reversed():
    df_pivoted = pl.DataFrame(
        {
            # "A": [1, 2, 3, 4, 5],
            "cars": ["beetle", "audi"],
            "beetle_banana": [2, None],
            "audi_banana": [None, 1],
            "beetle_apple": [2, None]
            # "B": [5, 4, 3, 2, 1],
        }
    )
    df_pivoted = df_pivoted.to_dicts()

    result = await _apply_pivot(
        df,
        index="cars",
        columns=["cars", "fruits"],
        value="A",
        aggregation_function="count",
    )
    result = result.to_dicts()

    assert True
    assert df_pivoted is not None
    assert result == df_pivoted


@pytest.mark.asyncio
async def test_advanced_pivoting_simple():
    df_pivoted = pl.DataFrame(
        {
            # "A": [1, 2, 3, 4, 5],
            "cars": ["beetle", "audi"],
            "banana": [2, 1],
            "apple": [2, None],
            # "B": [5, 4, 3, 2, 1],
        }
    )
    df_pivoted = df_pivoted.to_dicts()

    result: pl.DataFrame = await advanced_pivot(
        df,
        index="cars",
        columns="fruits",
        value_aggregations={"A": ["count"]}
        # value="A",
        # aggregation_function="count",
    )
    result = result.to_dicts()

    assert True
    assert df_pivoted is not None
    assert result == df_pivoted

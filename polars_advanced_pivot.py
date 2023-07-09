from functools import reduce
from typing import List, Dict, Optional, Union, Any
import asyncio
import polars as pl


async def _apply_pivot(
    df: pl.DataFrame,
    index: str,
    columns: Union[str, List[str]],
    value: str,
    aggregation_function: str,
) -> pl.DataFrame:
    # use good column naming
    if isinstance(columns, list):
        df = df.with_columns(pl.concat_str(*columns, separator="_").alias("pivotcol"))
        pivotcol = "pivotcol"
    else:
        pivotcol = columns
    df = df.pivot(
        value, index, columns=pivotcol, aggregate_function=aggregation_function
    )

    return df


async def advanced_pivot(
    df: pl.DataFrame,
    index: str,
    columns: Union[str, List[str]],
    value_aggregations: Dict[str, List[Any]],
) -> pl.DataFrame:
    # pivot aggregate the data in chunks / tasks
    tasks = []
    for value, aggfuncs in value_aggregations.items():
        for aggfunc in aggfuncs:
            tasks.append(
                asyncio.create_task(_apply_pivot(df, index, columns, value, aggfunc))
            )

    pivoted_data = await asyncio.gather(*tasks)
    # merge everything on the index column.

    def merge_df(left: pl.DataFrame, right: pl.DataFrame):
        return left.join(right, on=index, how="outer")

    all_data = reduce(merge_df, pivoted_data)

    return all_data

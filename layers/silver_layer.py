import polars as pl
import datetime
from loguru import logger
from pathlib import Path
from functools import reduce
from uuid import uuid4


def _get_input_path(
    base_input_path: Path,
    execution_time: datetime.datetime,
    prefix: str,
) -> Path:
    return base_input_path / execution_time.isoformat() / prefix


def _get_output_path(
    base_output_path: Path,
    execution_time: datetime.datetime,
) -> Path:
    write_path: Path = base_output_path / execution_time.isoformat()

    write_path.mkdir(parents=True, exist_ok=True)
    return write_path


def merge_and_clean_dataframes(
    base_input_path: Path,
    base_output_path: Path,
    prefixes: list[str],
    execution_time: datetime.datetime,
) -> None:
    logger.info("Merge all data into one table representation")
    dfs_list: list[pl.LazyFrame] = []
    for prefix in prefixes:
        input_path: Path = _get_input_path(
            base_input_path=base_input_path,
            execution_time=execution_time,
            prefix=prefix,
        )
        prefix_df: pl.LazyFrame = pl.scan_parquet(input_path)
        dfs_list.append(prefix_df)

    joint_df: pl.LazyFrame = reduce(
        lambda left_df, right_df: left_df.join(
            right_df, how="inner", on=["time", "city_name"]
        ),
        dfs_list,
    )

    adjusted_types_df: pl.LazyFrame = joint_df.cast({"time": pl.Datetime})

    adjusted_types_df.sink_parquet(
        _get_output_path(
            base_output_path=base_output_path, execution_time=execution_time
        )
        / f"{str(uuid4())}.parquet"
    )

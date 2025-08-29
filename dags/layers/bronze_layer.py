import polars as pl
import datetime
import json
from pathlib import Path
from loguru import logger


def _get_input_path(
    base_input_path: Path,
    execution_time: str,
    prefix: str,
    city_name: str,
) -> Path:
    return base_input_path / execution_time / prefix / f"{city_name}.json"


def _get_output_path(base_output_path: Path, execution_time: str, prefix: str) -> Path:
    write_path: Path = base_output_path / execution_time / prefix

    write_path.mkdir(parents=True, exist_ok=True)
    return write_path


def _transform_json_data_to_parquet(
    base_input_path: Path,
    base_output_path: Path,
    city_name: str,
    execution_time: str,
    prefix: str,
) -> None:
    input_path: Path = _get_input_path(
        base_input_path=base_input_path,
        execution_time=execution_time,
        prefix=prefix,
        city_name=city_name,
    )
    with open(input_path) as file:
        landing_zone_data: dict = json.load(file)

    content_df: pl.DataFrame = pl.from_dict(landing_zone_data["hourly"]).with_columns(
        pl.lit(city_name).alias("city_name")
    )

    write_path: Path = _get_output_path(
        base_output_path=base_output_path,
        execution_time=execution_time,
        prefix=prefix,
    )

    content_df.write_parquet(write_path / f"{city_name}.parquet")


def transform_weather_data_to_parquet(
    base_input_path: Path,
    base_output_path: Path,
    city_name: str,
    execution_time: str,
) -> None:
    logger.info(
        f"Transforming JSON data to parquet table representation for weather indicators for city {city_name}"
    )
    _transform_json_data_to_parquet(
        base_input_path=base_input_path,
        base_output_path=base_output_path,
        city_name=city_name,
        execution_time=execution_time,
        prefix="weather_data",
    )


def transform_air_quality_data_to_parquet(
    base_input_path: Path,
    base_output_path: Path,
    city_name: str,
    execution_time: str,
) -> None:
    logger.info(
        f"Transforming JSON data to parquet table representation for air quality indicators for city {city_name}"
    )
    _transform_json_data_to_parquet(
        base_input_path=base_input_path,
        base_output_path=base_output_path,
        city_name=city_name,
        execution_time=execution_time,
        prefix="air_quality_data",
    )

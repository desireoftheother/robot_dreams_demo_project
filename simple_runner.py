import datetime
from pathlib import Path
from dataclasses import dataclass

from layers.landing_zone import query_weather_api
from layers.landing_zone import query_air_quality_api
from layers.bronze_layer import transform_weather_data_to_parquet
from layers.bronze_layer import transform_air_quality_data_to_parquet
from layers.silver_layer import merge_and_clean_dataframes
from layers.golden_layer import calculate_analytics


@dataclass
class City:
    latitude: float
    longitude: float
    name: str


if __name__ == "__main__":
    current_dt: datetime.datetime = datetime.datetime.now()
    cities: list[City] = [
        City(50.450, 30.524, "Kyiv"),
        City(44.616, 33.525, "Sevastopol'"),
        City(48.015, 37.802, "Donets'k"),
    ]

    landing_zone_path: Path = Path.home() / "weather_pipeline/landing_zone"
    bronze_layer_path: Path = Path.home() / "weather_pipeline/bronze_layer"
    silver_layer_path: Path = Path.home() / "weather_pipeline/silver_layer"
    golden_layer_path: Path = Path.home() / "weather_pipeline/golden_layer"

    for city in cities:
        query_weather_api(
            base_output_path=landing_zone_path,
            longitude=city.longitude,
            latitude=city.latitude,
            city_name=city.name,
            execution_time=current_dt,
        )
        query_air_quality_api(
            base_output_path=landing_zone_path,
            longitude=city.longitude,
            latitude=city.latitude,
            city_name=city.name,
            execution_time=current_dt,
        )
        transform_weather_data_to_parquet(
            base_input_path=landing_zone_path,
            base_output_path=bronze_layer_path,
            city_name=city.name,
            execution_time=current_dt,
        )
        transform_air_quality_data_to_parquet(
            base_input_path=landing_zone_path,
            base_output_path=bronze_layer_path,
            city_name=city.name,
            execution_time=current_dt,
        )
    merge_and_clean_dataframes(
        base_input_path=bronze_layer_path,
        base_output_path=silver_layer_path,
        prefixes=["weather_data", "air_quality_data"],
        execution_time=current_dt,
    )
    calculate_analytics(
        base_input_path=silver_layer_path,
        base_output_path=golden_layer_path,
        execution_time=current_dt,
    )

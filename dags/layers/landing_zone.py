import datetime
import requests
from loguru import logger
from pathlib import Path


def _get_data_from_api(
    api_url: str,
    indicators: list[str],
    latitude: float,
    longitude: float,
    city_name: str,
    write_path: Path,
) -> None:
    query_params: dict[str, float | list[str]] = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": indicators,
    }
    api_response: requests.models.Response = requests.get(api_url, params=query_params)
    match api_response.status_code:
        case 200:
            with (write_path / f"{city_name}.json").open("w") as file:
                file.write(api_response.content.decode("utf-8"))
        case _:
            raise requests.HTTPError("Incorrect status code")


def _get_path_to_write(
    base_output_path: Path, execution_time: str, prefix: str
) -> Path:
    write_path: Path = base_output_path / execution_time / prefix
    write_path.mkdir(parents=True, exist_ok=True)
    return write_path


def query_weather_api(
    base_output_path: Path,
    execution_time: str,
    latitude: float,
    longitude: float,
    city_name: str,
    indicators: list[str] = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "wind_direction_10m",
    ],
    weather_api_url: str = "https://api.open-meteo.com/v1/forecast",
) -> None:
    logger.info(f"Getting weather data for {city_name}")
    write_path: Path = _get_path_to_write(
        base_output_path=base_output_path,
        execution_time=execution_time,
        prefix="weather_data",
    )
    _get_data_from_api(
        api_url=weather_api_url,
        indicators=indicators,
        latitude=latitude,
        longitude=longitude,
        city_name=city_name,
        write_path=write_path,
    )


def query_air_quality_api(
    base_output_path: Path,
    execution_time: str,
    latitude: float,
    longitude: float,
    city_name: str,
    indicators: list[str] = [
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "ozone",
    ],
    weather_api_url: str = "https://air-quality-api.open-meteo.com/v1/air-quality",
) -> None:
    logger.info(f"Getting air quality data for {city_name}")
    write_path: Path = _get_path_to_write(
        base_output_path=base_output_path,
        execution_time=execution_time,
        prefix="air_quality_data",
    )
    _get_data_from_api(
        api_url=weather_api_url,
        indicators=indicators,
        latitude=latitude,
        longitude=longitude,
        city_name=city_name,
        write_path=write_path,
    )

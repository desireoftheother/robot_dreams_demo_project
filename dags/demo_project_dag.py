from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

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


with DAG(dag_id="demo_project_dag", schedule="@hourly", catchup=False) as dag:
    cities: list[City] = [
        City(50.450, 30.524, "Kyiv"),
        City(44.616, 33.525, "Sevastopol"),
        City(48.015, 37.802, "Donetsk"),
    ]

    landing_zone_path: Path = Path.home() / "weather_pipeline/landing_zone"
    bronze_layer_path: Path = Path.home() / "weather_pipeline/bronze_layer"
    silver_layer_path: Path = Path.home() / "weather_pipeline/silver_layer"
    golden_layer_path: Path = Path.home() / "weather_pipeline/golden_layer"

    landing_tasks_weather: list[PythonOperator] = [
        PythonOperator(
            task_id=f"landing_zone_weather_api_{city.name.lower()}",
            python_callable=query_weather_api,
            op_kwargs={
                "base_output_path": landing_zone_path,
                "longitude": city.longitude,
                "latitude": city.latitude,
                "city_name": city.name,
                "execution_time": "{{ ds }}",
            },
        )
        for city in cities
    ]

    landing_tasks_air_quality: list[PythonOperator] = [
        PythonOperator(
            task_id=f"landing_zone_air_quality_api_{city.name.lower()}",
            python_callable=query_air_quality_api,
            op_kwargs={
                "base_output_path": landing_zone_path,
                "longitude": city.longitude,
                "latitude": city.latitude,
                "city_name": city.name,
                "execution_time": "{{ ds }}",
            },
        )
        for city in cities
    ]

    bronze_layer_weather: list[PythonOperator] = [
        PythonOperator(
            task_id=f"bronze_layer_weather_{city.name.lower()}",
            python_callable=transform_weather_data_to_parquet,
            op_kwargs={
                "base_input_path": landing_zone_path,
                "base_output_path": bronze_layer_path,
                "city_name": city.name,
                "execution_time": "{{ ds }}",
            },
        )
        for city in cities
    ]

    bronze_layer_air_quality: list[PythonOperator] = [
        PythonOperator(
            task_id=f"bronze_layer_air_quality_{city.name.lower()}",
            python_callable=transform_air_quality_data_to_parquet,
            op_kwargs={
                "base_input_path": landing_zone_path,
                "base_output_path": bronze_layer_path,
                "city_name": city.name,
                "execution_time": "{{ ds }}",
            },
        )
        for city in cities
    ]

    silver_layer_task: PythonOperator = PythonOperator(
        task_id="silver_layer",
        python_callable=merge_and_clean_dataframes,
        op_kwargs={
            "base_input_path": bronze_layer_path,
            "base_output_path": silver_layer_path,
            "prefixes": ["weather_data", "air_quality_data"],
            "execution_time": "{{ ds }}",
        },
    )

    golden_layer_task: PythonOperator = PythonOperator(
        task_id="golden_layer",
        python_callable=calculate_analytics,
        op_kwargs={
            "base_input_path": silver_layer_path,
            "base_output_path": golden_layer_path,
            "execution_time": "{{ ds }}",
        },
    )

    for landing, bronze in zip(landing_tasks_weather, bronze_layer_weather):
        landing >> bronze >> silver_layer_task

    for landing, bronze in zip(landing_tasks_air_quality, bronze_layer_air_quality):
        landing >> bronze >> silver_layer_task

    silver_layer_task >> golden_layer_task

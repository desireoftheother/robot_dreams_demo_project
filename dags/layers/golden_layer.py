import polars as pl
import datetime
from loguru import logger
from pathlib import Path


def analyze_precipitation_effect(df: pl.DataFrame) -> pl.DataFrame:
    """Analyze precipitation effects using Polars"""

    logger.info("\n🌧️ PRECIPITATION IMPACT ANALYSIS")
    logger.info("=" * 40)

    # Add precipitation flag
    df_with_flag = df.with_columns(
        [(pl.col("precipitation") > 0).alias("has_precipitation")]
    )

    # Group by precipitation presence
    precip_analysis = (
        df_with_flag.group_by("has_precipitation")
        .agg(
            [
                pl.col("pm2_5").mean().alias("avg_pm2_5"),
                pl.col("pm10").mean().alias("avg_pm10"),
                pl.col("ozone").mean().alias("avg_ozone"),
                pl.col("carbon_monoxide").mean().alias("avg_carbon_monoxide"),
                pl.len().alias("count"),
            ]
        )
        .sort("has_precipitation")
    )

    logger.info(precip_analysis)

    # Calculate improvement percentages
    no_rain = precip_analysis.filter(~pl.col("has_precipitation"))
    with_rain = precip_analysis.filter(pl.col("has_precipitation"))

    if no_rain.height > 0 and with_rain.height > 0:
        no_rain_pm25 = no_rain["avg_pm2_5"].item()
        with_rain_pm25 = with_rain["avg_pm2_5"].item()
        pm25_improvement = ((no_rain_pm25 - with_rain_pm25) / no_rain_pm25) * 100

        logger.info(f"\n📊 PM2.5 IMPROVEMENT WITH RAIN: {pm25_improvement:.1f}%")
        logger.info(f"   Without rain: {no_rain_pm25:.2f} μg/m³")
        logger.info(f"   With rain: {with_rain_pm25:.2f} μg/m³")

    return precip_analysis


def analyze_wind_effect(df: pl.DataFrame) -> pl.DataFrame:
    """Analyze wind speed effects using Polars"""

    logger.info("\n💨 WIND SPEED IMPACT ANALYSIS")
    logger.info("=" * 35)

    # Create wind speed categories
    df_with_wind_cat = df.with_columns(
        [
            pl.when(pl.col("wind_speed_10m") <= 3)
            .then(pl.lit("Light (0-3 km/h)"))
            .when(pl.col("wind_speed_10m") <= 7)
            .then(pl.lit("Gentle (3-7 km/h)"))
            .when(pl.col("wind_speed_10m") <= 12)
            .then(pl.lit("Moderate (7-12 km/h)"))
            .otherwise(pl.lit("Strong (>12 km/h)"))
            .alias("wind_category")
        ]
    )

    # Analyze by wind categories
    wind_analysis = (
        df_with_wind_cat.group_by("wind_category")
        .agg(
            [
                pl.col("pm2_5").mean().alias("avg_pm2_5"),
                pl.col("pm10").mean().alias("avg_pm10"),
                pl.col("ozone").mean().alias("avg_ozone"),
                pl.col("wind_speed_10m").mean().alias("avg_wind_speed"),
                pl.len().alias("count"),
            ]
        )
        .sort("avg_wind_speed")
    )

    logger.info(wind_analysis)

    # Calculate direct correlations
    wind_pm25_corr = df.select(
        [pl.corr("wind_speed_10m", "pm2_5").alias("wind_pm25_correlation")]
    ).item()

    wind_pm10_corr = df.select(
        [pl.corr("wind_speed_10m", "pm10").alias("wind_pm10_correlation")]
    ).item()

    logger.info("\n📈 DIRECT WIND CORRELATIONS:")
    logger.info(f"   Wind Speed ↔ PM2.5: {wind_pm25_corr:.3f}")
    logger.info(f"   Wind Speed ↔ PM10:  {wind_pm10_corr:.3f}")

    return wind_analysis


def analyze_hourly_patterns(df: pl.DataFrame) -> pl.DataFrame:
    """Analyze hourly patterns using Polars"""

    logger.info("\n⏰ HOURLY PATTERNS ANALYSIS")
    logger.info("=" * 30)

    # Add hour column
    df_with_hour = df.with_columns([pl.col("time").dt.hour().alias("hour")])

    # Group by hour
    hourly_stats = (
        df_with_hour.group_by("hour")
        .agg(
            [
                pl.col("pm2_5").mean().alias("avg_pm2_5"),
                pl.col("temperature_2m").mean().alias("avg_temperature_2m"),
                pl.col("wind_speed_10m").mean().alias("avg_wind_speed_10m"),
                pl.col("relative_humidity_2m").mean().alias("avg_relative_humidity_2m"),
                pl.len().alias("count"),
            ]
        )
        .sort("hour")
    )

    logger.info(hourly_stats)

    # Find peak pollution hours
    max_pm25_hour = hourly_stats.filter(
        pl.col("avg_pm2_5") == hourly_stats["avg_pm2_5"].max()
    )["hour"].item()

    min_pm25_hour = hourly_stats.filter(
        pl.col("avg_pm2_5") == hourly_stats["avg_pm2_5"].min()
    )["hour"].item()

    logger.info("\n🎯 KEY FINDINGS:")
    logger.info(f"   Peak PM2.5 hour: {max_pm25_hour}:00")
    logger.info(f"   Lowest PM2.5 hour: {min_pm25_hour}:00")

    return hourly_stats


def analyze_temperature_humidity_relationship(df: pl.DataFrame) -> pl.DataFrame:
    """Analyze temperature and humidity effects on air quality"""

    logger.info("\n🌡️ TEMPERATURE & HUMIDITY ANALYSIS")
    logger.info("=" * 40)

    # Create temperature categories
    df_with_temp_cat = df.with_columns(
        [
            pl.when(pl.col("temperature_2m") < 0)
            .then(pl.lit("Below 0°C"))
            .when(pl.col("temperature_2m") < 10)
            .then(pl.lit("0-10°C"))
            .when(pl.col("temperature_2m") < 20)
            .then(pl.lit("10-20°C"))
            .when(pl.col("temperature_2m") < 30)
            .then(pl.lit("20-30°C"))
            .otherwise(pl.lit("Above 30°C"))
            .alias("temp_category")
        ]
    )

    # Analyze by temperature ranges
    temp_analysis = (
        df_with_temp_cat.group_by("temp_category")
        .agg(
            [
                pl.col("pm2_5").mean().alias("avg_pm2_5"),
                pl.col("ozone").mean().alias("avg_ozone"),
                pl.col("temperature_2m").mean().alias("avg_temperature"),
                pl.len().alias("count"),
            ]
        )
        .sort("avg_temperature")
    )

    logger.info("📊 Air Quality by Temperature Range:")
    logger.info(temp_analysis)

    # Direct correlations
    temp_correlations = df.select(
        [
            pl.corr("temperature_2m", "pm2_5").alias("temp_pm25_corr"),
            pl.corr("temperature_2m", "ozone").alias("temp_ozone_corr"),
            pl.corr("relative_humidity_2m", "pm2_5").alias("humidity_pm25_corr"),
            pl.corr("relative_humidity_2m", "ozone").alias("humidity_ozone_corr"),
        ]
    )

    logger.info("\n📈 TEMPERATURE & HUMIDITY CORRELATIONS:")
    for row in temp_correlations.iter_rows(named=True):
        logger.info(f"   Temperature ↔ PM2.5: {row['temp_pm25_corr']:.3f}")
        logger.info(f"   Temperature ↔ Ozone: {row['temp_ozone_corr']:.3f}")
        logger.info(f"   Humidity ↔ PM2.5: {row['humidity_pm25_corr']:.3f}")
        logger.info(f"   Humidity ↔ Ozone: {row['humidity_ozone_corr']:.3f}")

    return temp_analysis


def generate_summary_statistics(df):
    """Generate comprehensive summary statistics"""

    logger.info("\n📈 SUMMARY STATISTICS")
    logger.info("=" * 25)

    # Basic descriptive statistics
    summary = df.select(
        [
            pl.col(
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "wind_speed_10m",
                "pm10",
                "pm2_5",
                "carbon_monoxide",
                "nitrogen_dioxide",
                "ozone",
            )
        ]
    ).describe()

    logger.info(summary)

    # Data quality metrics
    logger.info("\n🔍 DATA QUALITY METRICS:")
    logger.info(f"   Total records: {df.height:,}")
    logger.info(f"   Total columns: {df.width}")
    logger.info(
        f"   Date range: {(df['datetime'].max() - df['datetime'].min()).dt.total_days()} days"
    )

    # Null counts
    null_counts = df.null_count()
    total_nulls = null_counts.sum_horizontal().sum()
    data_completeness = (1 - total_nulls / (df.height * df.width)) * 100

    logger.info(f"   Data completeness: {data_completeness:.2f}%")
    logger.info(f"   Total null values: {total_nulls}")


def _get_output_path(
    base_output_path: Path,
    execution_time: str,
) -> Path:
    write_path: Path = base_output_path / execution_time

    write_path.mkdir(parents=True, exist_ok=True)
    return write_path


def calculate_analytics(
    base_input_path: Path,
    base_output_path: Path,
    execution_time: str,
) -> None:
    data_df: pl.DataFrame = pl.read_parquet(base_input_path / execution_time)

    precipitation_df: pl.DataFrame = analyze_precipitation_effect(data_df)
    wind_df: pl.DataFrame = analyze_wind_effect(data_df)
    hourly_patterns: pl.DataFrame = analyze_hourly_patterns(data_df)
    humidity_df: pl.DataFrame = analyze_temperature_humidity_relationship(data_df)

    output_path: Path = _get_output_path(
        base_output_path=base_output_path, execution_time=execution_time
    )

    precipitation_df.write_csv(output_path / "precipitation.csv")
    wind_df.write_csv(output_path / "wind.csv")
    hourly_patterns.write_csv(output_path / "hourly_patterns.csv")
    humidity_df.write_csv(output_path / "humidity_df.csv")

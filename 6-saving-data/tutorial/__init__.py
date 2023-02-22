from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    fs_io_manager
)

from dagster_duckdb import build_duckdb_io_manager
from dagster_duckdb_pandas import DuckDBPandasTypeHandler

from . import assets

# Fetch the assets
hackernews_assets = load_assets_from_modules([assets])

# Define a job that will materialize the assets
hackernews_job = define_asset_job(
    "hackernews_job",
    selection=AssetSelection.groups("hackernews")
)

# Define a schedule that will run the job every hour
hackernews_schedule = ScheduleDefinition(
    job=hackernews_job,
    cron_schedule="0 * * * *" # every hour
)

io_manager = fs_io_manager.configured({
    "base_dir": "hacker_news", # Path is built relative to where `dagster dev` is run
})

# Define the I/O manager to tell Dagster how to load and store the assets
duckdb_io_manager = build_duckdb_io_manager([DuckDBPandasTypeHandler()]).configured({
    "database": "analytics.hackernews"
})

defs = Definitions(
    assets=[*hackernews_assets],
    schedules=[hackernews_schedule],
    resources={
        "io_manager": io_manager,
        "database_io_manager": duckdb_io_manager
    }
)

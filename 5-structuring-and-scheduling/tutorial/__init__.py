from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets

# Fetch the assets
hackernews_assets = load_assets_from_modules([assets])

# Define a job that will materialize the assets
hackernews_job = define_asset_job(
    "hackernews_job",
    selection=AssetSelection.all()
)

# Define a schedule that will run the job every hour
hackernews_schedule = ScheduleDefinition(
    job=hackernews_job,
    cron_schedule="0 * * * *" # every hour
)

defs = Definitions(
    assets=[*hackernews_assets],
    schedules=[hackernews_schedule]
)

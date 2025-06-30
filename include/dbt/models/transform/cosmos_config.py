from cosmos.config import ProfileConfig, ProjectConfig # type: ignore
from pathlib import Path

DBT_CONFIG = ProfileConfig(
    profile_name='bookings',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/include/dbt/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/include/dbt/',
)
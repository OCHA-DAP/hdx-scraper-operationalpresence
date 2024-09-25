"""Entry point to start HAPI HNO pipeline"""

import logging
from os import getenv
from os.path import expanduser, join
from typing import Optional

from ._version import __version__
from .pipeline import Pipeline
from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.dateparse import now_utc
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file, temp_dir

setup_logging()
logger = logging.getLogger(__name__)

lookup = "hdx-scraper-operationalpresence"
updated_by_script = "HDX Scraper: Operational Presence"


def main(
    save: bool = False,
    use_saved: bool = False,
    countryiso3s: str = "",
    save_test_data: bool = False,
    gsheet_auth: Optional[str] = None,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
        countryiso3s (str): Countries to process. Defaults to "" (all countries).
        save_test_data (bool): Whether to save test data. Defaults to False.
        gsheet_auth (Optional[str]): Google Sheets authorisation. Defaults to None.
    Returns:
        None
    """
    logger.info(f"##### {lookup} version {__version__} ####")
    if not User.check_current_user_organization_access(
        "hdx", "create_dataset"
    ):
        raise PermissionError(
            "API Token does not give access to HDX organisation!"
        )
    with temp_dir() as temp_folder:
        configuration = Configuration.read()
        today = now_utc()
        Read.create_readers(
            temp_folder,
            "saved_data",
            temp_folder,
            save,
            use_saved,
            hdx_auth=configuration.get_api_key(),
            today=today,
        )
        if gsheet_auth is None:
            gsheet_auth = getenv("GSHEET_AUTH")
        pipeline = Pipeline(configuration, countryiso3s, gsheet_auth)
        pipeline.find_datasets_resources()
        pipeline.process()

    logger.info("HDX Scraper Operational Presence pipeline completed!")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )

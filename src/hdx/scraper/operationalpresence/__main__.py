"""Entry point to start HAPI HNO pipeline"""

import logging
from os import getenv
from os.path import expanduser, join
from typing import Optional

from hdx.api.utilities.hdx_error_handler import HDXErrorHandler

from ._version import __version__
from .pipeline import Pipeline
from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.framework.utilities.reader import Read
from hdx.scraper.operationalpresence.sheet import Sheet
from hdx.utilities.dateparse import now_utc
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file, temp_dir

setup_logging()
logger = logging.getLogger(__name__)

lookup = "hdx-scraper-operationalpresence"
updated_by_script = "HDX Scraper: Operational Presence"


def main(
    gsheet_auth: Optional[str] = None,
    email_server: Optional[str] = None,
    recipients: Optional[str] = None,
    countryiso3s: str = "",
    save: bool = False,
    use_saved: bool = False,
    err_to_hdx: bool = False,
) -> None:
    """Generate datasets and create them in HDX.

    An optional authorisation string for Google Sheets can be supplied of the
    form:
    {"type": "service_account", "project_id": "hdx-bot", "private_key_id": ...
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",...}

    An optional email server can be supplied in the form:
    connection type (eg. ssl),host,port,username,password,sender email
    If not supplied, no emails will be sent.

    recipients is a list of email addresses of the people who should be emailed
    when new datasets or resources are detected.

    Args:
        gsheet_auth (Optional[str]): Google Sheets authorisation. Defaults to None.
        email_server (Optional[str]): Email server to use. Defaults to None.
        recipients (Optional[str]): Email addresses. Defaults to None.
        countryiso3s (str): Countries to process. Defaults to "" (all countries).
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
        err_to_hdx (bool): Whether to write any errors to HDX metadata. Defaults to False.
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
    with HDXErrorHandler(should_exit_on_error=False) as error_handler:
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
            if email_server is None:
                email_server = getenv("EMAIL_SERVER")
            if recipients is None:
                recipients = getenv("RECIPIENTS")
            sheet = Sheet(configuration, gsheet_auth, email_server, recipients)
            pipeline = Pipeline(configuration, sheet, error_handler, countryiso3s)
            pipeline.find_datasets_resources()
            countryiso3s, startdate, enddate = pipeline.process()
            dataset = pipeline.generate_org_dataset(temp_folder)
            if dataset:
                dataset.add_other_location("World")
                dataset.set_time_period(startdate, enddate)
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        join("config", "hdx_dataset_static.yaml"), main
                    )
                )
                dataset["notes"] = (
                    "This dataset contains standardised Organisation data"
                )
                dataset.create_in_hdx(
                    remove_additional_resources=False,
                    hxl_update=False,
                    updated_by_script=updated_by_script,
                )
            dataset = pipeline.generate_3w_dateset(temp_folder)
            if dataset:
                dataset.add_country_locations(countryiso3s)
                dataset.set_time_period(startdate, enddate)
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        join("config", "hdx_dataset_static.yaml"), main
                    )
                )
                dataset["notes"] = (
                    "This dataset contains standardised Operational Presence data"
                )
                dataset.create_in_hdx(
                    remove_additional_resources=False,
                    hxl_update=False,
                    updated_by_script=updated_by_script,
                )
            pipeline.output_errors(err_to_hdx)
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

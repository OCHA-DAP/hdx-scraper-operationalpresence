import logging
from datetime import datetime, timezone
from os import getenv
from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.vocabulary import Vocabulary
from hdx.scraper.framework.utilities.reader import Read
from hdx.scraper.operationalpresence.pipeline import Pipeline
from hdx.scraper.operationalpresence.sheet import Sheet
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import script_dir_plus_file, temp_dir
from hdx.utilities.useragent import UserAgent

logger = logging.getLogger(__name__)


class TestOperationalPresence:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=script_dir_plus_file(
                join("config", "project_configuration.yaml"), Pipeline
            ),
        )
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
                {"name": "bdi", "title": "Burundi"},
                {"name": "lbn", "title": "Lebanon"},
                {"name": "world", "title": "World"},
            ]
        )
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": tag}
                for tag in (
                    "hxl",
                    "operational presence",
                )
            ],
            "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            "name": "approved",
        }
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    def test_main(
        self,
        configuration,
        fixtures_dir,
        input_dir,
    ):
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "TestOperationalPresence",
                delete_on_success=True,
                delete_on_failure=False,
            ) as temp_folder:
                configuration = Configuration.read()
                today = parse_date("09/01/2025")
                Read.create_readers(
                    temp_folder,
                    input_dir,
                    temp_folder,
                    False,
                    True,
                    today=today,
                )
                gsheet_auth = getenv("GSHEET_AUTH")
                sheet = Sheet(
                    configuration, gsheet_auth, None, None, "spreadsheet_test"
                )
                countryiso3s = "BDI,LBN"
                pipeline = Pipeline(
                    configuration, sheet, error_handler, countryiso3s
                )
                pipeline.find_datasets_resources()
                countryiso3s, startdate, enddate = pipeline.process()
                assert countryiso3s == ["BDI", "LBN"]
                assert startdate == datetime(
                    2021, 3, 31, 0, 0, tzinfo=timezone.utc
                )
                assert enddate == datetime(
                    2024, 9, 2, 23, 59, 59, 999999, tzinfo=timezone.utc
                )

                dataset = pipeline.generate_org_dataset(temp_folder)
                assert dataset == {
                    "data_update_frequency": "90",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "global-organisations",
                    "owner_org": "hdx",
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        }
                    ],
                    "title": "Global Organisations",
                }
                assert dataset.get_resources() == [
                    {
                        "name": "Organisations",
                        "description": "Global organisation data with HXL hashtags",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]
                filename = "organisations.csv"
                expected_file = join(fixtures_dir, filename)
                actual_file = join(temp_folder, filename)
                assert_files_same(expected_file, actual_file)

                dataset = pipeline.generate_3w_dateset(temp_folder)
                assert dataset == {
                    "data_update_frequency": "90",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "global-operational-presence",
                    "owner_org": "hdx",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "operational presence",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Global Operational Presence",
                }
                assert dataset.get_resources() == [
                    {
                        "name": "Operational Presence",
                        "description": "Global Operational Presence data with HXL hashtags",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]
                filename = "operational_presence.csv"
                expected_file = join(fixtures_dir, filename)
                actual_file = join(temp_folder, filename)
                assert_files_same(expected_file, actual_file)

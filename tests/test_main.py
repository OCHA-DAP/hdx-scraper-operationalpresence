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
                pipeline.process()
                assert sorted(pipeline._iso3_to_datasetinfo.keys()) == [
                    "BDI",
                    "LBN",
                ]
                assert pipeline._start_date == datetime(
                    2021, 3, 31, 0, 0, tzinfo=timezone.utc
                )
                assert pipeline._end_date == datetime(
                    2024, 9, 2, 23, 59, 59, 999999, tzinfo=timezone.utc
                )

                dataset = pipeline.generate_org_dataset(temp_folder)
                assert dataset == {
                    "data_update_frequency": "30",
                    "dataset_date": "[2021-03-31T00:00:00 TO 2024-09-02T23:59:59]",
                    "dataset_source": "Humanitarian partners",
                    "groups": [{"name": "world"}],
                    "license_id": "cc-by-igo",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "hdx-hapi-organisations",
                    "owner_org": "40d10ece-49de-4791-9aed-e164f1d16dd1",
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        }
                    ],
                    "title": "HDX HAPI - Coordination & Context: Organisations",
                }
                assert dataset.get_resources() == [
                    {
                        "description": "Organisation data from HDX HAPI",
                        "format": "csv",
                        "name": "Global Coordination & Context: Organisations",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]
                filename = "hdx_hapi_organisations_global.csv"
                expected_file = join(fixtures_dir, filename)
                actual_file = join(temp_folder, filename)
                assert_files_same(expected_file, actual_file)

                dataset = pipeline.generate_3w_dataset(temp_folder)
                assert dataset == {
                    "data_update_frequency": "30",
                    "dataset_date": "[2021-03-31T00:00:00 TO 2024-09-02T23:59:59]",
                    "dataset_source": "OCHA Burundi,OCHA Lebanon",
                    "groups": [{"name": "bdi"}, {"name": "lbn"}],
                    "license_id": "hdx-other",
                    "license_other": "[Creative Commons Attribution "
                    "International](http://www.opendefinition.org/licenses/cc-by),[Creative "
                    "Commons Attribution for Intergovernmental "
                    "Organisations](http://creativecommons.org/licenses/by/3.0/igo/legalcode)",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "hdx-hapi-operational-presence",
                    "owner_org": "40d10ece-49de-4791-9aed-e164f1d16dd1",
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
                    "title": "HDX HAPI - Coordination & Context: Operational Presence",
                }
                assert dataset.get_resources() == [
                    {
                        "description": "Who is doing What Where - Operational Presence data from HDX "
                        "HAPI, please see [the "
                        "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/coordination_and_context/#operational-presence) "
                        "for more information",
                        "format": "csv",
                        "name": "Global Coordination & Context: Operational Presence",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]
                filename = "hdx_hapi_operational_presence_global.csv"
                expected_file = join(fixtures_dir, filename)
                actual_file = join(temp_folder, filename)
                assert_files_same(expected_file, actual_file)

                expected_file = join(fixtures_dir, "org_map.csv")
                actual_file = pipeline._org.output_org_map(temp_folder)
                assert_files_same(expected_file, actual_file)

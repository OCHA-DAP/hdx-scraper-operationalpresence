import logging
from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.dataset import Dataset
from hdx.data.vocabulary import Vocabulary
from hdx.scraper.operationalpresence.pipeline import Pipeline
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.useragent import UserAgent

logger = logging.getLogger(__name__)


class TestHAPIPipelineHNO:
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
                {"name": "sdn", "title": "Sudan"},
                {"name": "world", "title": "World"},
            ]
        )
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": tag}
                for tag in (
                    "hxl",
                    "humanitarian needs overview - hno",
                    "people in need - pin",
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

    @pytest.fixture(scope="function")
    def input_dataset_afg(self, configuration, input_dir):
        return Dataset.load_from_json(
            join(input_dir, "afghanistan-humanitarian-needs.json")
        )

    @pytest.fixture(scope="function")
    def input_dataset_sdn(self, configuration, input_dir):
        return Dataset.load_from_json(
            join(input_dir, "sudan-humanitarian-needs.json")
        )

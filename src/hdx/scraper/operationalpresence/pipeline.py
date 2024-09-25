from logging import getLogger
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.location.adminlevel import AdminLevel
from hdx.scraper.framework.utilities.reader import Read

from hdx.scraper.operationalpresence.sheet import Sheet

logger = getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        countryiso3s_to_process: str = "",
        gsheet_auth: Optional[str] = None
    ) -> None:
        self.configuration = configuration
        if countryiso3s_to_process:
            self.countryiso3s_to_process = countryiso3s_to_process.split(",")
        else:
            self.countryiso3s_to_process = None
        self.sheet = Sheet(configuration, gsheet_auth)
        self.adminone = AdminLevel(admin_level=1)
        self.adminone.setup_from_url(countryiso3s=self.countryiso3s_to_process)
        self.adminone.load_pcode_formats()
        self.admintwo = AdminLevel(admin_level=2)
        self.admintwo.setup_from_url(countryiso3s=self.countryiso3s_to_process)
        self.admintwo.load_pcode_formats()
        self.global_rows = {}
        self.reader = Read.get_reader("hdx")

    def find_datasets_resources(self):
        datasets_found = self.reader.search_datasets(
            "operational_presence", fq='vocab_Topics:"operational presence"'
        )
        datasets_by_iso3 = {}
        for dataset in datasets_found:
            countryiso3s = dataset.get_location_iso3s()
            if len(countryiso3s) != 1:
                continue
            countryiso3 = countryiso3s[0]
            if countryiso3 == "WORLD":
                continue
            if (
                self.countryiso3s_to_process
                and countryiso3 not in self.countryiso3s_to_process
            ):
                continue
            if dataset.get("archived", False):
                continue
            if dataset.get("dataseries_name") in self.configuration["dataseries_ignore"]:
                continue
            if any(x in self.configuration["tags_ignore"] for x in dataset.get_tags()):
                continue
            if any(x in dataset["name"].lower() for x in self.configuration["words_ignore"]):
                continue
            allowed_format = False
            for resource in dataset.get_resources():
                if resource.get_format() in self.configuration["allowed_formats"]:
                    allowed_format = True
                    break
            if not allowed_format:
                continue
            existing_dataset = datasets_by_iso3.get(countryiso3)
            if existing_dataset:
                existing_enddate = existing_dataset.get_time_period()[
                    "enddate"
                ]
                enddate = dataset.get_time_period()["enddate"]
                if enddate > existing_enddate:
                    datasets_by_iso3[countryiso3] = dataset
            else:
                datasets_by_iso3[countryiso3] = dataset

        for countryiso3 in sorted(datasets_by_iso3):
            dataset = datasets_by_iso3[countryiso3]
            resources = dataset.get_resources()
            resource_to_process = resources[0]
            latest_last_modified = resource_to_process["last_modified"]
            for resource in resources[1:]:
                last_modified = resource["last_modified"]
                if last_modified > latest_last_modified:
                    latest_last_modified = last_modified
                    resource_to_process = resource
            dataset_name = dataset["name"]
            resource_name = resource_to_process["name"]
            resource_format = resource_to_process.get_format()
            self.sheet.add_update_row(countryiso3, dataset_name, resource_name, resource_format)
        self.sheet.write(list(datasets_by_iso3.keys()))

    def process_country(self, countryiso3: str, dataset: str, resource: str, format: str) -> None:
        datasetinfo = {"dataset": dataset, "resource": resource, "format": format}
        self.reader.read_tabular(datasetinfo)

    def process(self) -> None:
        for countryiso3 in self.sheet.get_countries():
            dataset, resource, format = self.sheet.get_dataset_resource(countryiso3)
            self.process_country(countryiso3, dataset, resource, format)

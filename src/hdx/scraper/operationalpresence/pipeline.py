from datetime import datetime
from logging import getLogger
from typing import Dict, List, Optional, Set, Tuple

from slugify import slugify

from .logging_helpers import add_message, add_missing_value_message
from .org import Org, OrgInfo
from .org_type import OrgType
from .sector import Sector
from .sheet import Sheet
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.adminlevel import AdminLevel
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
    iso_string_from_datetime,
    parse_date,
)
from hdx.utilities.text import normalise

logger = getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        countryiso3s_to_process: str = "",
        gsheet_auth: Optional[str] = None,
    ) -> None:
        self._configuration = configuration
        if countryiso3s_to_process:
            self._countryiso3s_to_process = countryiso3s_to_process.split(",")
        else:
            self._countryiso3s_to_process = None
        self._sheet = Sheet(configuration, gsheet_auth)

        self._reader = Read.get_reader("hdx")
        self._admins = []
        for i in range(3):
            admin = AdminLevel(admin_level=i + 1, retriever=self._reader)
            if i == 2:
                admin.setup_from_url(
                    admin_url=configuration["global_all_pcodes"],
                    countryiso3s=self._countryiso3s_to_process,
                )
            else:
                admin.setup_from_url(
                    countryiso3s=self._countryiso3s_to_process
                )
            admin.load_pcode_formats()
            self._admins.append(admin)
        self._global_rows = {}
        self._org = Org(
            datasetinfo=configuration["org"],
        )
        self._org_type = OrgType(
            datasetinfo=configuration["org_type"],
            org_type_map=configuration["org_type_map"],
        )
        self._sector = Sector(
            datasetinfo=configuration["sector"],
            sector_map=configuration["sector_map"],
        )
        self._rows = []

    def find_datasets_resources(self):
        datasets_found = self._reader.search_datasets(
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
                self._countryiso3s_to_process
                and countryiso3 not in self._countryiso3s_to_process
            ):
                continue
            if dataset.get("archived", False):
                continue
            if (
                dataset.get("dataseries_name")
                in self._configuration["dataseries_ignore"]
            ):
                continue
            if any(
                x in self._configuration["tags_ignore"]
                for x in dataset.get_tags()
            ):
                continue
            if any(
                x in dataset["name"].lower()
                for x in self._configuration["words_ignore"]
            ):
                continue
            allowed_format = False
            for resource in dataset.get_resources():
                if (
                    resource.get_format()
                    in self._configuration["allowed_formats"]
                ):
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
            resource_to_process = None
            latest_last_modified = default_date
            for resource in dataset.get_resources():
                if (
                    resource.get_format()
                    not in self._configuration["allowed_formats"]
                ):
                    continue
                last_modified = parse_date(resource["last_modified"])
                if last_modified > latest_last_modified:
                    latest_last_modified = last_modified
                    resource_to_process = resource
            dataset_name = dataset["name"]
            resource_name = resource_to_process["name"]
            resource_format = resource_to_process.get_format()
            self._sheet.add_update_row(
                countryiso3, dataset_name, resource_name, resource_format
            )
        self._sheet.write(list(datasets_by_iso3.keys()))

    def complete_org_info(
        self,
        org_info: OrgInfo,
        org_acronym: Optional[str],
        org_type_name: Optional[str],
        errors: Set[str],
        dataset_name: str,
    ):
        if org_info.acronym is None and org_acronym is not None:
            if len(org_acronym) > 32:
                org_acronym = org_acronym[:32]
            org_info.acronym = org_acronym
            org_info.normalised_acronym = normalise(org_acronym)

        # * Org type processing
        if org_info.type_code is None and org_type_name is not None:
            org_type_code = self._org_type.get_org_type_code(org_type_name)
            if org_type_code:
                org_info.type_code = org_type_code
            else:
                add_missing_value_message(
                    errors,
                    dataset_name,
                    "org type",
                    org_type_name,
                )

        # * Org matching
        self._org.add_or_match_org(org_info)

    def preprocess_country(
        self, countryiso3: str, datasetinfo: Dict, errors: Set[str]
    ):
        dataset_name = datasetinfo["dataset"]
        adm_code_cols = datasetinfo["Adm Code Columns"]
        adm_name_cols = datasetinfo["Adm Name Columns"]
        org_name_col = datasetinfo["Org Name Column"]
        org_acronym_col = datasetinfo["Org Acronym Column"]
        org_type_col = datasetinfo["Org Type Column"]
        sector_col = datasetinfo["Sector Column"]
        headers, iterator = self._reader.read(datasetinfo)
        if datasetinfo["use_hxl"]:
            header_to_hxltag = next(iterator)
            hxltag_to_header = {v: k for k, v in header_to_hxltag.items()}
            if adm_code_cols:
                new_adm_code_cols = []
                for adm_code_col in adm_code_cols.split(","):
                    if adm_code_col:
                        new_adm_code_cols.append(
                            hxltag_to_header[adm_code_col]
                        )
                    else:
                        new_adm_code_cols.append("")
                adm_code_cols = ",".join(new_adm_code_cols)
            new_adm_name_cols = []
            for adm_name_col in adm_name_cols.split(","):
                if adm_name_col:
                    new_adm_name_cols.append(hxltag_to_header[adm_name_col])
                else:
                    new_adm_name_cols.append("")
            adm_name_cols = ",".join(new_adm_name_cols)
            org_name_col = hxltag_to_header[org_name_col]
            if org_acronym_col:
                org_acronym_col = hxltag_to_header[org_acronym_col]
            if org_type_col:
                org_type_col = hxltag_to_header[org_type_col]
            sector_col = hxltag_to_header[sector_col]
            datasetinfo["Adm Code Columns"] = adm_code_cols
            datasetinfo["Adm Name Columns"] = adm_name_cols
            datasetinfo["Org Name Column"] = org_name_col
            datasetinfo["Org Acronym Column"] = org_acronym_col
            datasetinfo["Org Type Column"] = org_type_col
            datasetinfo["Sector Column"] = sector_col
        norows = 0
        for row in iterator:
            norows += 1
            org_str = row[org_name_col]
            org_acronym = row[org_acronym_col]
            if not org_str:
                org_str = org_acronym

            # * Sector processing
            sector_orig = row[sector_col]
            # Skip rows that are missing a sector
            if not sector_orig:
                add_message(
                    errors,
                    dataset_name,
                    f"org {org_str} missing sector",
                )
                continue
            sector_code = self._sector.get_sector_code(sector_orig)
            if not sector_code:
                continue

            # * Org processing
            org_info = self._org.get_org_info(org_str, location=countryiso3)
            if not org_info.complete:
                if org_type_col:
                    org_type_name = row[org_type_col]
                else:
                    org_type_name = None
                self.complete_org_info(
                    org_info,
                    org_acronym,
                    org_type_name,
                    errors,
                    dataset_name,
                )
        logger.info(f"{norows} rows preprocessed from {dataset_name}")

    def process_country(
        self, countryiso3: str, datasetinfo: Dict, errors: Set[str]
    ) -> Tuple[datetime, datetime]:
        dataset_name = datasetinfo["dataset"]
        start_date = datasetinfo["Start Date"]
        if start_date:
            end_date = datasetinfo["End Date"]
            datasetinfo["source_date"] = {"start": start_date, "end": end_date}
        adm_code_cols = datasetinfo["Adm Code Columns"]
        if adm_code_cols:
            adm_code_cols = adm_code_cols.split(",")
        adm_name_cols = datasetinfo["Adm Name Columns"].split(",")
        org_name_col = datasetinfo["Org Name Column"]
        org_acronym_col = datasetinfo["Org Acronym Column"]
        sector_col = datasetinfo["Sector Column"]
        headers, iterator = self._reader.read(datasetinfo)
        start_date = datasetinfo["source_date"]["default_date"]["start"]
        end_date = datasetinfo["source_date"]["default_date"]["end"]
        norowsin = 0
        norowsout = 0
        for row in iterator:
            sector_orig = row[sector_col]
            if sector_orig[0] == "#":
                continue
            norowsin += 1
            if not sector_orig:
                continue
            # Skip rows that are missing a sector
            sector_code = self._sector.get_sector_code(sector_orig)
            if not sector_code:
                continue
            # * Org processing
            org_str = row[org_name_col]
            org_acronym = row[org_acronym_col]
            if not org_str:
                org_str = org_acronym
            org_info = self._org.get_org_info(org_str, location=countryiso3)

            adm_codes = ["", "", ""]
            adm_names = ["", "", ""]
            prev_pcode = None
            for i, adm_name_col in reversed(list(enumerate(adm_name_cols))):
                adm_names[i] = row[adm_name_col]
                if adm_code_cols:
                    adm_code_col = adm_code_cols[i]
                    if adm_code_col:
                        pcode = row[adm_code_cols[i]]
                    else:
                        pcode = None
                    if not pcode and prev_pcode:
                        pcode = self._admins[i + 1].pcode_to_parent[prev_pcode]
                    adm_codes[i] = pcode
                    prev_pcode = pcode

            parent = None
            for i, adm_code in enumerate(adm_codes):
                if adm_code:
                    continue
                adm_name = adm_names[i]
                if not adm_name:
                    continue
                adm_code, _ = self._admins[i].get_pcode(
                    countryiso3, adm_name, parent=parent
                )
                if adm_code:
                    adm_codes[i] = adm_code
                    parent = adm_code

            output_row = {
                "Country ISO3": countryiso3,
                "Admin 1 Code": adm_codes[0],
                "Admin 1 Name": adm_names[0],
                "Admin 2 Code": adm_codes[1],
                "Admin 2 Name": adm_names[1],
                "Admin 3 Code": adm_codes[2],
                "Admin 3 Name": adm_names[2],
                "Org Name": org_info.canonical_name,
                "Org Acronym": org_info.acronym,
                "Org Type": org_info.type_code,
                "Sector": sector_code,
                "Start Date": iso_string_from_datetime(start_date),
                "End Date": iso_string_from_datetime(end_date),
            }
            self._rows.append(output_row)
            norowsout += 1
        logger.info(
            f"{norowsin} rows processed from {dataset_name} producing {norowsout} rows."
        )
        return start_date, end_date

    def process(self) -> Tuple[List, datetime, datetime]:
        self._org.populate()
        self._org_type.populate()
        self._sector.populate()
        earliest_start_date = default_enddate
        latest_end_date = default_date
        iso3_to_datasetinfo = {}
        errors = set()
        for countryiso3 in self._sheet.get_countries():
            datasetinfo = self._sheet.get_datasetinfo(countryiso3)
            if datasetinfo:
                iso3_to_datasetinfo[countryiso3] = datasetinfo
                self.preprocess_country(countryiso3, datasetinfo, errors)
        for countryiso3, datasetinfo in iso3_to_datasetinfo.items():
            start_date, end_date = self.process_country(
                countryiso3, datasetinfo, errors
            )
            if start_date < earliest_start_date:
                earliest_start_date = start_date
            if end_date > latest_end_date:
                latest_end_date = end_date
        countryiso3s = list(iso3_to_datasetinfo.keys())
        return countryiso3s, earliest_start_date, latest_end_date

    def generate_3w_dateset(self, folder: str) -> Optional[Dataset]:
        title = "Global Operational Presence"
        logger.info(f"Creating dataset: {title}")
        slugified_name = slugify(title).lower()
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("hdx")
        dataset.set_expected_update_frequency("Every three months")

        tags = [
            "hxl",
            "operational presence",
        ]
        dataset.add_tags(tags)

        dataset.set_subnational(True)

        resourcedata = {
            "name": "Operational Presence",
            "description": "Global Operational Presence data with HXL hashtags",
        }
        filename = "operational_presence.csv"

        hxltags = self._configuration["hxltags"]
        success, results = dataset.generate_resource_from_iterable(
            list(hxltags.keys()),
            sorted(
                self._rows,
                key=lambda x: (
                    x["Country ISO3"],
                    x["Admin 1 Code"],
                    x["Admin 2 Code"],
                    x["Admin 3 Code"],
                ),
            ),
            hxltags,
            folder,
            filename,
            resourcedata,
        )
        if success is False:
            logger.warning(f"{title} has no data!")
            return None
        return dataset

    def generate_org_dataset(self, folder: str) -> Optional[Dataset]:
        title = "Global Organisations"
        logger.info(f"Creating dataset: {title}")
        slugified_name = slugify(title).lower()
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("hdx")
        dataset.set_expected_update_frequency("Every three months")
        dataset.add_tag("hxl")

        dataset.set_subnational(False)

        resourcedata = {
            "name": "Organisations",
            "description": "Global organisation data with HXL hashtags",
        }
        filename = "organisations.csv"

        hxltags = self._configuration["org_hxltags"]
        org_rows = [
            {
                "Acronym": org_data.acronym,
                "Name": org_data.name,
                "Org Type Code": org_data.type_code,
            }
            for org_data in sorted(self._org.data.values())
        ]
        success, results = dataset.generate_resource_from_iterable(
            list(hxltags.keys()),
            org_rows,
            hxltags,
            folder,
            filename,
            resourcedata,
        )
        if success is False:
            logger.warning(f"{title} has no data!")
            return None
        return dataset

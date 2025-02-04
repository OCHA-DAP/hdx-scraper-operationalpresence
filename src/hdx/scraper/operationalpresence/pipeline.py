import re
import traceback
from datetime import datetime
from logging import getLogger
from typing import Dict, List, NamedTuple, Optional, Tuple

from hdx.location.country import Country
from slugify import slugify

from .org import Org
from .sheet import Sheet
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.adminlevel import AdminLevel
from hdx.scraper.framework.utilities.reader import Read
from hdx.scraper.framework.utilities.sector import Sector
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
    iso_string_from_datetime,
    parse_date,
)
from hdx.utilities.dictandlist import invert_dictionary
from hdx.utilities.matching import multiple_replace

logger = getLogger(__name__)


# eg. row['#date+year']=='2024' and row['#date+quarter']=='Q3'
ROW_LOOKUP = re.compile(r"row\[['\"](.*?)['\"]\]")


class Row(NamedTuple):
    location_code: str
    has_hrp: str
    in_gho: str
    provider_admin1_name: str
    provider_admin2_name: str
    provider_admin3_name: str
    admin1_code: str
    admin1_name: str
    admin2_code: str
    admin2_name: str
    admin3_code: str
    admin3_name: str
    admin_level: int
    org_name: str
    org_acronym: str
    org_type_code: str
    org_type_description: str
    sector_code: str
    sector_name: str
    reference_period_start: str
    reference_period_end: str
    dataset_id: str
    resource_id: str
    error: str


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        sheet: Sheet,
        error_handler: HDXErrorHandler,
        countryiso3s_to_process: str = "",
    ) -> None:
        self._configuration = configuration
        self._sheet = sheet
        self._error_handler = error_handler
        if countryiso3s_to_process:
            self._countryiso3s_to_process = countryiso3s_to_process.split(",")
        else:
            self._countryiso3s_to_process = None

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
        self._org = Org(
            datasetinfo=configuration["org"],
            error_handler=error_handler,
        )
        self._sector = Sector()
        self._rows = set()
        self._errors = set()

    def get_format_from_url(self, resource: Resource) -> Optional[str]:
        format = resource["url"][-4:].lower()
        if format not in self._configuration["allowed_formats"]:
            format = resource["url"][-3:].lower()
        if format in self._configuration["allowed_formats"]:
            return format
        return None

    def get_format(
        self, dataset_name: str, resource: Resource
    ) -> Tuple[bool, str]:
        resource_name = resource["name"]
        hdx_format = resource.get_format()
        # handle erroneously entered HDX format
        format = self.get_format_from_url(resource)
        if format:
            if format != hdx_format:
                self._error_handler.add_message(
                    "OperationalPresence",
                    dataset_name,
                    f"Resource {resource_name} has url with format {format} that is different to HDX format {hdx_format}",
                    resource_name,
                    "error",
                    True,
                )
        else:
            format = hdx_format
        if format in self._configuration["allowed_formats"]:
            return True, format
        return False, format

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
            resource_url_format = self.get_format_from_url(resource_to_process)
            self._sheet.add_update_row(
                countryiso3,
                dataset_name,
                resource_name,
                resource_format,
                resource_url_format,
            )
        self._sheet.write(list(datasets_by_iso3.keys()))
        self._sheet.send_email()

    def preprocess_country(self, countryiso3: str, datasetinfo: Dict) -> bool:
        dataset_name = datasetinfo["dataset"]
        adm_code_cols = datasetinfo["Adm Code Columns"]
        adm_name_cols = datasetinfo["Adm Name Columns"]
        org_name_col = datasetinfo["Org Name Column"]
        org_acronym_col = datasetinfo["Org Acronym Column"]
        org_type_col = datasetinfo["Org Type Column"]
        sector_col = datasetinfo["Sector Column"]
        start_date = datasetinfo["Start Date"]
        if start_date:
            end_date = datasetinfo["End Date"]
            datasetinfo["source_date"] = {"start": start_date, "end": end_date}
        resource = self._reader.read_hdx_metadata(datasetinfo)
        resource_name = resource["name"]
        rows = []
        success, format = self.get_format(dataset_name, resource)
        if not success:
            self._error_handler.add_message(
                "OperationalPresence",
                dataset_name,
                f"Resource {resource_name} has format {format} which is not allowed",
                message_type="warning",
            )
            return False
        filename = self._reader.construct_filename(resource_name, format)
        datasetinfo["filename"] = filename
        datasetinfo["format"] = format
        headers, iterator = self._reader.read_tabular(datasetinfo)
        filter = datasetinfo["Filter"]
        if datasetinfo["use_hxl"]:
            header_to_hxltag = next(iterator)
            hxltag_to_header = invert_dictionary(header_to_hxltag)
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

            if filter:
                replace = {}
                for match in ROW_LOOKUP.finditer(filter):
                    hxltag = match.group(1)
                    header = hxltag_to_header[hxltag]
                    replace[hxltag] = header
                filter = multiple_replace(filter, replace)
                datasetinfo["Filter"] = filter

        norows = 0
        for row in iterator:
            if filter:
                if not eval(filter):
                    continue
            # Skip HXL tag row
            hxlrow = False
            for value in row.values():
                if value and value[0] == "#":
                    hxlrow = True
                    break
            if hxlrow:
                continue
            norows += 1
            row["Error"] = []
            org_str = row[org_name_col]
            org_acronym = row[org_acronym_col]
            if not org_str:
                org_str = org_acronym
            # Skip rows with no org name or acronym
            if not org_str:
                self._error_handler.add_message(
                    "OperationalPresence",
                    dataset_name,
                    f"row {norows} missing organisation",
                )
                row["Error"].append("No org!")

            # * Sector processing
            sector_orig = row[sector_col]
            # Skip rows that are missing a sector
            if not sector_orig:
                self._error_handler.add_message(
                    "OperationalPresence",
                    dataset_name,
                    f"org {org_str} missing sector",
                )
                row["Error"].append("No sector!")
            sector_code = self._sector.get_code(sector_orig)
            if sector_code:
                row[sector_col] = sector_code
            else:
                self._error_handler.add_missing_value_message(
                    "OperationalPresence",
                    dataset_name,
                    "sector",
                    sector_orig,
                )
                row["Error"].append(f"Unknown sector {sector_orig}!")
            rows.append(row)
            if row["Error"]:
                continue

            # * Org processing
            org_info = self._org.get_org_info(org_str, location=countryiso3)
            if not org_info.complete:
                if org_type_col:
                    org_type_name = row[org_type_col]
                else:
                    org_type_name = None
                self._org.complete_org_info(
                    org_info,
                    org_acronym,
                    org_type_name,
                    dataset_name,
                )
            # * Org matching
            self._org.add_or_match_org(org_info)

        logger.info(f"{norows} rows preprocessed from {dataset_name}")
        datasetinfo["rows"] = rows
        return True

    def get_adm_info(
        self,
        countryiso3: str,
        row: Dict,
        adm_code_cols: List[str],
        adm_name_cols: List[str],
        dataset_name: str,
    ) -> Tuple[List, List, List, int]:
        provider_adm_names = ["", "", ""]
        adm_codes = ["", "", ""]
        adm_names = ["", "", ""]
        prev_pcode = None
        adm_level = 0
        for i, adm_name_col in reversed(list(enumerate(adm_name_cols))):
            if adm_name_col:
                provider_adm_name = row[adm_name_col]
                if provider_adm_name:
                    provider_adm_name = provider_adm_name.strip()
                    if provider_adm_name:
                        provider_adm_names[i] = provider_adm_name
                        if i >= adm_level:
                            adm_level = i + 1
            if adm_code_cols:
                adm_code_col = adm_code_cols[i]
                if adm_code_col:
                    pcode = row[adm_code_cols[i]]
                    if pcode:
                        if pcode in self._admins[i].pcodes:
                            if i >= adm_level:
                                adm_level = i + 1
                        else:
                            self._error_handler.add_missing_value_message(
                                "OperationalPresence",
                                dataset_name,
                                f"admin {i+1} pcode",
                                pcode,
                            )
                            pcode = None
                            row["Error"].append(f"Unknown pcode {pcode}!")
                else:
                    pcode = None
                if not pcode and prev_pcode:
                    pcode = self._admins[i + 1].pcode_to_parent.get(prev_pcode)
                if not pcode:
                    pcode = ""
                adm_codes[i] = pcode
                prev_pcode = pcode

        parent = None
        for i, adm_code in enumerate(adm_codes):
            if adm_code:
                adm_name = self._admins[i].pcode_to_name.get(adm_code, "")
                adm_names[i] = adm_name
                continue
            provider_adm_name = provider_adm_names[i]
            if not provider_adm_name:
                continue
            adm_code, _ = self._admins[i].get_pcode(
                countryiso3, provider_adm_name, parent=parent
            )
            if adm_code:
                adm_codes[i] = adm_code
                adm_name = self._admins[i].pcode_to_name.get(adm_code, "")
                adm_names[i] = adm_name
                parent = adm_code
        return provider_adm_names, adm_codes, adm_names, adm_level

    def process_country(
        self, countryiso3: str, datasetinfo: Dict
    ) -> Tuple[datetime, datetime]:
        dataset_name = datasetinfo["dataset"]
        adm_code_cols = datasetinfo["Adm Code Columns"]
        if adm_code_cols:
            adm_code_cols = adm_code_cols.split(",")
        adm_name_cols = datasetinfo["Adm Name Columns"].split(",")
        org_name_col = datasetinfo["Org Name Column"]
        org_acronym_col = datasetinfo["Org Acronym Column"]
        sector_col = datasetinfo["Sector Column"]
        start_date = datasetinfo["time_period"]["start"]
        end_date = datasetinfo["time_period"]["end"]
        start_date_str = iso_string_from_datetime(start_date)
        end_date_str = iso_string_from_datetime(end_date)
        output_rows = set()
        rows = datasetinfo["rows"]
        for row in rows:
            sector_code = row[sector_col]
            # * Org processing
            org_str = row[org_name_col]
            org_acronym = row[org_acronym_col]
            if not org_str:
                org_str = org_acronym
            if org_str:
                org_info = self._org.get_org_info(
                    org_str, location=countryiso3
                )
            else:
                org_info = self._org.get_blank_org_info()

            # * Adm processing
            provider_adm_names, adm_codes, adm_names, adm_level = (
                self.get_adm_info(
                    countryiso3,
                    row,
                    adm_code_cols,
                    adm_name_cols,
                    dataset_name,
                )
            )

            dataset_id = datasetinfo["hapi_dataset_metadata"]["hdx_id"]
            resource_id = datasetinfo["hapi_resource_metadata"]["hdx_id"]

            output_row = Row(
                countryiso3,
                "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N",
                "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N",
                provider_adm_names[0],
                provider_adm_names[1],
                provider_adm_names[2],
                adm_codes[0],
                adm_names[0],
                adm_codes[1],
                adm_names[1],
                adm_codes[2],
                adm_names[2],
                adm_level,
                org_info.canonical_name,
                org_info.acronym,
                org_info.type_code,
                self._org.get_org_type_description(org_info.type_code),
                sector_code,
                self._sector.get_code_to_name().get(sector_code, ""),
                start_date_str,
                end_date_str,
                dataset_id,
                resource_id,
                "".join(row["Error"]),
            )
            output_rows.add(output_row)
        logger.info(
            f"{len(rows)} rows processed from {dataset_name} producing {len(output_rows)} rows."
        )
        self._rows.update(output_rows)
        del datasetinfo["rows"]
        return start_date, end_date

    def process(self) -> Tuple[List, datetime, datetime]:
        self._org.populate()
        earliest_start_date = default_enddate
        latest_end_date = default_date
        iso3_to_datasetinfo = {}
        for countryiso3 in self._sheet.get_countries():
            if (
                self._countryiso3s_to_process
                and countryiso3 not in self._countryiso3s_to_process
            ):
                continue
            datasetinfo = self._sheet.get_datasetinfo(countryiso3)
            if datasetinfo:
                try:
                    success = self.preprocess_country(countryiso3, datasetinfo)
                    if success:
                        iso3_to_datasetinfo[countryiso3] = datasetinfo
                except Exception:
                    self._error_handler.add_message(
                        "OperationalPresence",
                        datasetinfo["dataset"],
                        traceback.format_exc(),
                    )

        for countryiso3, datasetinfo in iso3_to_datasetinfo.items():
            start_date, end_date = self.process_country(
                countryiso3, datasetinfo
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

        rows = sorted(self._rows)
        if len(rows) == 0:
            logger.warning(f"{title} has no data!")
            return None
        dataset.generate_resource_from_rows(
            folder,
            filename,
            [list(hxltags.keys())]
            + [list(hxltags.values())]
            + sorted(self._rows),
            resourcedata,
        )
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
                "acronym": org_data.acronym,
                "name": org_data.name,
                "org_type_code": org_data.type_code,
                "org_type_description": self._org.get_org_type_description(org_data.type_code)
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

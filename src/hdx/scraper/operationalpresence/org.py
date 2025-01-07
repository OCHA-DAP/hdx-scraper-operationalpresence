"""Populate the org table."""

import logging
from dataclasses import dataclass
from os.path import join
from typing import Dict, NamedTuple, Optional, Set

from hdx.scraper.framework.utilities.reader import Read
from hdx.scraper.operationalpresence.logging_helpers import (
    add_missing_value_message,
)
from hdx.scraper.operationalpresence.org_type import OrgType
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.text import normalise

logger = logging.getLogger(__name__)


@dataclass
class OrgInfo:
    canonical_name: str
    normalised_name: str
    acronym: str  # can be ""
    normalised_acronym: str  # can be ""
    type_code: str  # can be ""
    used: bool = False
    complete: bool = False


class OrgData(NamedTuple):
    acronym: str
    name: str
    type_code: str


class Org:
    def __init__(
        self,
        datasetinfo: Dict[str, str],
        org_type: OrgType,
    ):
        self._datasetinfo = datasetinfo
        self._org_type = org_type
        self.data = {}
        self._org_map = {}

    def populate(self) -> None:
        logger.info("Populating org mapping")
        reader = Read.get_reader()
        headers, iterator = reader.get_tabular_rows(
            self._datasetinfo["url"],
            headers=2,
            dict_form=True,
            format="csv",
            file_prefix="org",
        )

        for i, row in enumerate(iterator):
            canonical_name = row["#org+name"]
            if not canonical_name:
                logger.error(f"Canonical name is empty in row {i}!")
                continue
            normalised_name = normalise(canonical_name)
            country_code = row["#country+code"]
            acronym = row["#org+acronym"]
            if acronym:
                normalised_acronym = normalise(acronym)
            else:
                normalised_acronym = None
            org_name = row["#x_pattern"]
            type_code = row["#org+type+code"]
            org_info = OrgInfo(
                canonical_name,
                normalised_name,
                acronym,
                normalised_acronym,
                type_code,
            )
            self._org_map[(country_code, canonical_name)] = org_info
            self._org_map[(country_code, normalised_name)] = org_info
            self._org_map[(country_code, acronym)] = org_info
            self._org_map[(country_code, normalised_acronym)] = org_info
            self._org_map[(country_code, org_name)] = org_info
            self._org_map[(country_code, normalise(org_name))] = org_info

    def get_org_info(self, org_str: str, location: str) -> OrgInfo:
        key = (location, org_str)
        org_info = self._org_map.get(key)
        if org_info:
            return org_info
        normalised_str = normalise(org_str)
        org_info = self._org_map.get((location, normalised_str))
        if org_info:
            self._org_map[key] = org_info
            return org_info
        org_info = self._org_map.get((None, org_str))
        if org_info:
            self._org_map[key] = org_info
            return org_info
        org_info = self._org_map.get((None, normalised_str))
        if org_info:
            self._org_map[key] = org_info
            return org_info
        org_info = OrgInfo(
            canonical_name=org_str,
            normalised_name=normalised_str,
            acronym="",
            normalised_acronym="",
            type_code="",
        )
        self._org_map[key] = org_info
        return org_info

    def add_or_match_org(self, org_info: OrgInfo) -> OrgData:
        key = (org_info.normalised_acronym, org_info.normalised_name)
        org_data = self.data.get(key)
        if org_data:
            if not org_data.type_code and org_info.type_code:
                org_data = OrgData(
                    org_data.acronym, org_data.name, org_info.type_code
                )
                self.data[key] = org_data
                # TODO: should we flag orgs if we find more than one org type?
            else:
                org_info.type_code = org_data.type_code
            # Since we're looking up by normalised acronym and normalised name,
            # these don't need copying here
            org_info.acronym = org_data.acronym
            org_info.canonical_name = org_data.name

        else:
            org_data = OrgData(
                org_info.acronym, org_info.canonical_name, org_info.type_code
            )
            self.data[key] = org_data
        if org_info.acronym and org_info.type_code:
            org_info.complete = True
        org_info.used = True
        return org_data

    def complete_org_info(
        self,
        org_info: OrgInfo,
        org_acronym: Optional[str],
        org_type_name: Optional[str],
        errors: Set[str],
        dataset_name: str,
    ):
        if not org_info.acronym and org_acronym:
            if len(org_acronym) > 32:
                org_acronym = org_acronym[:32]
            org_info.acronym = org_acronym
            org_info.normalised_acronym = normalise(org_acronym)

        # * Org type processing
        if not org_info.type_code and org_type_name:
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
        self.add_or_match_org(org_info)

    def output_org_map(self, folder: str) -> None:
        rows = [
            (
                "Country Code",
                "Lookup",
                "Canonical Name",
                "Normalised Name",
                "Acronym",
                "Normalised Acronym",
                "Type Code",
                "Used",
                "Complete",
            )
        ]
        for key, org_info in self._org_map.items():
            country_code, lookup = key
            rows.append(
                (
                    country_code,
                    lookup,
                    org_info.canonical_name,
                    org_info.normalised_name,
                    org_info.acronym,
                    org_info.normalised_acronym,
                    org_info.type_code,
                    "Y" if org_info.used else "N",
                    "Y" if org_info.complete else "N",
                )
            )
        write_list_to_csv(join(folder, "org_map.csv"), rows)

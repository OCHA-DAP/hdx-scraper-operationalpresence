"""Populate the org type mapping."""

import logging
from typing import Dict

from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.matching import get_code_from_name
from hdx.utilities.text import normalise

logger = logging.getLogger(__name__)


class OrgType:
    def __init__(
        self,
        datasetinfo: Dict[str, str],
        org_type_map: Dict[str, str],
    ):
        self._datasetinfo = datasetinfo
        self.data = org_type_map
        self.unmatched = []

    def populate(self) -> None:
        logger.info("Populating org type mapping")

        def parse_org_type_values(code: str, description: str) -> None:
            self.data[code] = code
            self.data[description] = code
            self.data[normalise(code)] = code
            self.data[normalise(description)] = code

        reader = Read.get_reader()
        headers, iterator = reader.read(
            self._datasetinfo, file_prefix="org_type"
        )
        for row in iterator:
            parse_org_type_values(
                code=row["#org +type +code +v_hrinfo"],
                description=row["#org +type +preferred"],
            )

        extra_entries = {
            "501": "Civil Society",
            "502": "Observer",
            "503": "Development Programme",
            "504": "Local NGO",
        }
        for code, description in extra_entries.items():
            parse_org_type_values(
                code=code,
                description=description,
            )

    def get_org_type_code(self, org_type: str) -> str | None:
        return get_code_from_name(
            name=org_type,
            code_lookup=self.data,
            unmatched=self.unmatched,
        )

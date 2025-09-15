import re
from typing import Dict, Optional, Tuple

from hdx.data.resource import Resource
from hdx.utilities.dateparse import (
    parse_date_range,
)

# eg. afghanistan-3w-operational-presence-april-june-2025.csv
DATES_IN_FILENAME = re.compile(
    r"([a-zA-Z]+)[\s\-_]+(?:to)?[\s\-_]*([a-zA-Z]+)[\s\-_]+(\d\d\d\d).*"
)


def get_dates_from_filename(
    resource: Resource, country_info: Optional[Dict]
) -> Tuple[str, str]:
    if not country_info:
        return "", ""
    resource_name = resource["name"]
    filename_dates = country_info["Filename Dates"]
    if filename_dates.lower() == "y":
        match = DATES_IN_FILENAME.search(resource_name)
        if match:
            start_month = match.group(1)
            end_month = match.group(2)
            year = match.group(3)
            start_date_str = f"{start_month}-{year}"
            end_date_str = f"{end_month}-{year}"
            start_date, _ = parse_date_range(start_date_str)
            _, end_date = parse_date_range(end_date_str)
            return start_date.strftime("%d/%m/%Y"), end_date.strftime("%d/%m/%Y")
    return "", ""

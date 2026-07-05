import re

from dateutil.parser import ParserError
from hdx.data.resource import Resource
from hdx.utilities.dateparse import (
    parse_date_range,
)

# eg. afghanistan-3w-operational-presence-april-june-2025.csv
DATES_IN_FILENAME = re.compile(
    r"([a-zA-Z]+)[\s\-_]+(?:to)?[\s\-_]*([a-zA-Z]+)[\s\-_]+(\d{4}).*"
)

# eg. NER_Oct_2025.csv, TCD_3W_Dec2025.xlsx, 3W_CAR_Juin 2025.xlsx
SINGLE_DATE_IN_FILENAME = re.compile(r"([a-zA-Z]+)[\s\-_]*(\d{4})(?!\d)")

FRENCH_MONTH_MAP = {
    "janvier": "January",
    "janv": "January",
    "fevrier": "February",
    "fevr": "February",
    "fev": "February",
    "mars": "March",
    "avril": "April",
    "avr": "April",
    "mai": "May",
    "juin": "June",
    "juillet": "July",
    "juil": "July",
    "aout": "August",
    "septembre": "September",
    "octobre": "October",
    "novembre": "November",
    "decembre": "December",
}


def translate_month(month_str: str) -> str:
    return FRENCH_MONTH_MAP.get(month_str.lower(), month_str)


def get_dates_from_filename(
    resource: Resource, country_info: dict | None
) -> tuple[bool, str, str]:
    if not country_info:
        return False, "", ""
    resource_name = resource["name"]
    filename_dates = country_info["Filename Dates"]
    if filename_dates.lower() == "y":
        match = DATES_IN_FILENAME.search(resource_name)
        if match:
            start_month = translate_month(match.group(1))
            end_month = translate_month(match.group(2))
            year = match.group(3)
            if not start_month or not end_month or not year:
                return True, "", ""
            start_date_str = f"{start_month}-{year}"
            end_date_str = f"{end_month}-{year}"
            try:
                start_date, _ = parse_date_range(start_date_str)
                _, end_date = parse_date_range(end_date_str)
                return (
                    False,
                    start_date.strftime("%d/%m/%Y"),
                    end_date.strftime("%d/%m/%Y"),
                )
            except ParserError:
                pass
        for match in SINGLE_DATE_IN_FILENAME.finditer(resource_name):
            month = translate_month(match.group(1))
            year = match.group(2)
            date_str = f"{month}-{year}"
            try:
                start_date, end_date = parse_date_range(date_str)
                return (
                    False,
                    start_date.strftime("%d/%m/%Y"),
                    end_date.strftime("%d/%m/%Y"),
                )
            except ParserError:
                continue
    return False, "", ""

import json
from logging import getLogger
from typing import Dict, Iterable, List, Optional

import gspread

from hdx.api.configuration import Configuration
from hdx.utilities.email import Email

logger = getLogger(__name__)


class Sheet:
    headers = [
        "Country ISO3",
        "Exclude",
        "Automated Dataset",
        "Automated Resource",
        "Automated Format",
        "Automated Start Date",
        "Automated End Date",
        "Dataset",
        "Resource",
        "Format",
        "Sheet",
        "Headers",
        "Start Date",
        "End Date",
        "Filter",
        "Filename Dates",
        "Start Date Column",
        "End Date Column",
        "Adm Code Columns",
        "Adm Name Columns",
        "Org Name Column",
        "Org Acronym Column",
        "Org Type Column",
        "Sector Column",
    ]

    def __init__(
        self,
        configuration: Configuration,
        gsheet_auth: Optional[str] = None,
        email_server: Optional[str] = None,
        recipients: Optional[str] = None,
        gsheet_key: str = "spreadsheet",
    ):
        self._configuration = configuration
        self.spreadsheet_rows = {}
        self.sheet = None
        if gsheet_auth:
            self.read_existing(gsheet_auth, gsheet_key)
        if email_server:  # Get email server details
            email_config = email_server.split(",")
            email_config_dict = {
                "connection_type": email_config[0],
                "host": email_config[1],
                "port": int(email_config[2]),
                "username": email_config[3],
                "password": email_config[4],
            }
            if len(email_config) <= 5:
                email_config_dict["sender"] = "operationalpresence@humdata.org"
            self._emailer = Email(email_config_dict=email_config_dict)
            logger.info(f"> Email host: {email_config[1]}")
            self._recipients = recipients.split(",")
        else:
            self._emailer = None
            self._recipients = None
        self.email_text = []

    def read_existing(self, gsheet_auth: str, gsheet_key: str) -> None:
        try:
            info = json.loads(gsheet_auth)
            scopes = ["https://www.googleapis.com/auth/spreadsheets"]
            gc = gspread.service_account_from_dict(info, scopes=scopes)
            logger.info(f"Opening operational presence datasets {gsheet_key}")
            self.spreadsheet = gc.open_by_url(self._configuration[gsheet_key])
            self.sheet = self.spreadsheet.get_worksheet(0)
            gsheet_rows = self.sheet.get_values()
            for row in gsheet_rows[1:]:
                new_row = {header: row[i] for i, header in enumerate(self.headers)}
                countryiso3 = new_row["Country ISO3"]
                self.spreadsheet_rows[countryiso3] = new_row
        except Exception as ex:
            logger.error(ex)

    def get_countries(self) -> Iterable[str]:
        return self.spreadsheet_rows.keys()

    def get_country_row(self, countryiso3: str) -> Optional[Dict]:
        return self.spreadsheet_rows.get(countryiso3)

    def add_update_row(
        self,
        countryiso3: str,
        dataset_name: str,
        resource_name: str,
        resource_format: str,
        resource_url_format: Optional[str],
        filename_dates_broken: bool,
    ) -> None:
        row = self.get_country_row(countryiso3)
        if row is None:
            changed = True
            row = {
                "Country ISO3": countryiso3,
                "Exclude": "",
                "Automated Dataset": dataset_name,
                "Automated Resource": resource_name,
                "Automated Format": resource_format,
            }
            self.spreadsheet_rows[countryiso3] = row
        else:
            changed = False
            current_dataset = row["Automated Dataset"]
            if current_dataset != dataset_name:
                changed = True
                text = f"{countryiso3}: Updating dataset from {current_dataset} to {dataset_name}"
                logger.info(text)
                self.email_text.append(text)
                row["Automated Dataset"] = dataset_name
            current_resource = row["Automated Resource"]
            if current_resource != resource_name:
                changed = True
                text = f"{countryiso3}: Updating resource from {current_resource} to {resource_name}"
                logger.info(text)
                self.email_text.append(text)
                row["Automated Resource"] = resource_name
            current_format = row["Automated Format"]
            if current_format != resource_format:
                changed = True
                text = f"{countryiso3}: Updating resource format from {current_format} to {resource_format}"
                logger.info(text)
                self.email_text.append(text)
                row["Automated Format"] = resource_format
            if filename_dates_broken:
                changed = True
                text = f"{countryiso3}: Filename dates broken. Turned off flag. Please check resource!"
                logger.warning(text)
                self.email_text.append(text)
                row["Filename Dates"] = ""
        if changed and resource_url_format and resource_url_format != resource_format:
            text = f"Resource {resource_name} has url with format {resource_url_format} that is different to HDX format {resource_format}"
            logger.warning(text)
            self.email_text.append(text)

    def add_update_dates(
        self,
        countryiso3: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> None:
        row = self.get_country_row(countryiso3)
        current_start_date = row.get("Automated Start Date")
        if current_start_date is None:
            row["Automated Start Date"] = ""
        elif current_start_date != start_date:
            text = f"{countryiso3}: Updating start date from {current_start_date} to {start_date}"
            logger.info(text)
            self.email_text.append(text)
            row["Automated Start Date"] = start_date
        current_end_date = row.get("Automated End Date")
        if current_end_date is None:
            row["Automated End Date"] = ""
        elif current_end_date != end_date:
            text = f"{countryiso3}: Updating end date from {current_end_date} to {end_date}"
            logger.info(text)
            self.email_text.append(text)
            row["Automated End Date"] = end_date

    def write(self, countryiso3s: List) -> None:
        if self.sheet is None:
            return
        rows = [self.headers]
        for countryiso3 in sorted(self.spreadsheet_rows):
            row = self.spreadsheet_rows[countryiso3]
            if countryiso3 not in countryiso3s:
                row["Automated Dataset"] = ""
                row["Automated Resource"] = ""
                row["Automated Format"] = ""
                row["Automated Start Date"] = ""
                row["Automated End Date"] = ""
            rows.append([row[header] for header in self.headers])
        sheet_copy = self.sheet.get_all_values()
        try:
            self.sheet.clear()
            self.sheet.update("A1", rows)
        except Exception as ex:
            logger.exception(
                "Error updating Google Sheet! Trying to restore old values", ex
            )
            self.sheet.update("A1", sheet_copy)

    def send_email(self) -> None:
        if self._emailer is None or len(self.email_text) == 0:
            return
        self._emailer.send(
            self._recipients,
            "Operational presence - updates detected!",
            "\n".join(self.email_text),
        )

    def get_datasetinfo(self, countryiso3: str) -> Optional[Dict]:
        row = self.spreadsheet_rows[countryiso3]
        if row["Exclude"] == "Y":
            return None
        automated_dataset = row["Automated Dataset"]
        dataset = row["Dataset"]
        if dataset:
            logger.info(
                f"Using override dataset {dataset} instead of {automated_dataset} for {countryiso3}"
            )
        else:
            dataset = automated_dataset
        automated_resource = row["Automated Resource"]
        resource = row["Resource"]
        if resource:
            logger.info(
                f"Using override resource {resource} instead of {automated_resource} for {countryiso3}"
            )
        else:
            resource = automated_resource
        format = row["Format"]
        automated_format = row["Automated Format"]
        if format:
            logger.info(
                f"Using override format {format} instead of {automated_format} for {countryiso3}"
            )
        else:
            format = automated_format
        datasetinfo = {
            "name": countryiso3,
            "dataset": dataset,
            "resource": resource,
            "format": format,
        }
        sheet = row["Sheet"]
        if sheet:
            datasetinfo["sheet"] = sheet
        headers = row["Headers"]
        if headers:
            if "," in headers:
                headers = headers.split(",")
                for i, header in enumerate(headers):
                    headers[i] = int(header)
            else:
                headers = int(headers)
            datasetinfo["headers"] = headers
        if format == "xlsx":
            datasetinfo["xlsx2csv"] = True
        start_date = row["Start Date"]
        if not start_date:
            start_date = row["Automated Start Date"]
        if start_date:
            end_date = row["End Date"]
            if not end_date:
                end_date = row["Automated End Date"]
            datasetinfo["source_date"] = {"start": start_date, "end": end_date}
        for header in self.headers[14:]:
            datasetinfo[header] = row[header]
        # Config must contain an org name and a sector
        if not datasetinfo["Org Name Column"]:
            logger.warning(
                f"Ignoring {countryiso3} from config spreadsheet because it has no Org Name Column!"
            )
            return {}
        if not datasetinfo["Sector Column"]:
            logger.warning(
                f"Ignoring {countryiso3} from config spreadsheet because it has no Sector Column!"
            )
            return {}
        # Config must contain either adm code or adm name columns
        if not datasetinfo["Adm Code Columns"] and not datasetinfo["Adm Name Columns"]:
            logger.warning(
                f"Ignoring {countryiso3} from config spreadsheet because it has no Adm Code Columns and no Adm Name Columns!"
            )
            return {}
        if datasetinfo["Org Name Column"][0] == "#":
            use_hxl = True
        else:
            use_hxl = False
        datasetinfo["use_hxl"] = use_hxl
        # If no acronym column defined use org name column as acronym column
        if not datasetinfo["Org Acronym Column"]:
            datasetinfo["Org Acronym Column"] = datasetinfo["Org Name Column"]
        return datasetinfo

import json
from logging import getLogger
from typing import Dict, Iterable, List, Optional

import gspread

from hdx.api.configuration import Configuration
from hdx.utilities.email import Email

logger = getLogger(__name__)


class Sheet:
    iso3_ind = 0
    automated_dataset_ind = 1
    automated_resource_ind = 2
    automated_format_ind = 3
    dataset_ind = 4
    resource_ind = 5
    format_ind = 6
    sheet_ind = 7
    headers_ind = 8
    # start_date_ind = 9
    # end_date_ind = 10
    # filter_ind = 11
    # admin_codes_ind = 12
    # admin_names_ind = 13
    # org_name_ind = 14
    # org_acronym_ind = 15
    # org_type_ind = 16
    # sector_ind = 17
    headers = [
        "Country ISO3",
        "Automated Dataset",
        "Automated Resource",
        "Automated Format",
        "Dataset",
        "Resource",
        "Format",
        "Sheet",
        "Headers",
        "Start Date",
        "End Date",
        "Filter",
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
            if len(email_config) > 5:
                email_config_dict["sender"] = email_config[5]
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
            logger.info("Opening operational presence datasets gsheet")
            self.spreadsheet = gc.open_by_url(self._configuration[gsheet_key])
            self.sheet = self.spreadsheet.get_worksheet(0)
            gsheet_rows = self.sheet.get_values()
            for row in gsheet_rows[1:]:
                countryiso3 = row[self.iso3_ind]
                self.spreadsheet_rows[countryiso3] = row
        except Exception as ex:
            logger.error(ex)

    def add_update_row(
        self,
        countryiso3: str,
        dataset_name: str,
        resource_name: str,
        resource_format: str,
        resource_url_format: Optional[str],
    ) -> None:
        if resource_url_format and resource_url_format != resource_format:
            text = f"Resource {resource_name} has url with format {resource_url_format} that is different to HDX format {resource_format}"
            logger.warning(text)
            self.email_text.append(text)
        row = self.spreadsheet_rows.get(countryiso3)
        if row is None:
            row = [countryiso3, dataset_name, resource_name, resource_format]
            self.spreadsheet_rows[countryiso3] = row
        else:
            current_dataset = row[self.automated_dataset_ind]
            if current_dataset != dataset_name:
                text = f"{countryiso3}: Updating dataset from {current_dataset} to {dataset_name}"
                logger.info(text)
                self.email_text.append(text)
                row[self.automated_dataset_ind] = dataset_name
            current_resource = row[self.automated_resource_ind]
            if current_resource != resource_name:
                text = f"{countryiso3}: Updating resource from {current_resource} to {resource_name}"
                logger.info(text)
                self.email_text.append(text)
                row[self.automated_resource_ind] = resource_name
            current_format = row[self.automated_format_ind]
            if current_format != resource_format:
                text = f"{countryiso3}: Updating resource format from {current_format} to {resource_format}"
                logger.info(text)
                self.email_text.append(text)
                row[self.automated_format_ind] = resource_format

    def write(self, countryiso3s: List) -> None:
        if self.sheet is None:
            return
        rows = [self.headers]
        for countryiso3 in sorted(self.spreadsheet_rows):
            row = self.spreadsheet_rows[countryiso3]
            if countryiso3 not in countryiso3s:
                row[self.automated_dataset_ind] = ""
                row[self.automated_resource_ind] = ""
            rows.append(row)
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

    def get_countries(self) -> Iterable[str]:
        return self.spreadsheet_rows.keys()

    def get_datasetinfo(self, countryiso3: str) -> Dict:
        row = self.spreadsheet_rows[countryiso3]
        automated_dataset = row[self.automated_dataset_ind]
        dataset = row[self.dataset_ind]
        if dataset:
            logger.info(
                f"Using override dataset {dataset} instead of {automated_dataset} for {countryiso3}"
            )
        else:
            dataset = automated_dataset
        automated_resource = row[self.automated_resource_ind]
        resource = row[self.resource_ind]
        if resource:
            logger.info(
                f"Using override resource {resource} instead of {automated_resource} for {countryiso3}"
            )
        else:
            resource = automated_resource
        format = row[self.format_ind]
        automated_format = row[self.automated_format_ind]
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
        sheet = row[self.sheet_ind]
        if sheet:
            datasetinfo["sheet"] = sheet
        headers = row[self.headers_ind]
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
        for i, header in enumerate(self.headers[9:]):
            datasetinfo[header] = row[i + 9]
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
        if (
            not datasetinfo["Adm Code Columns"]
            and not datasetinfo["Adm Name Columns"]
        ):
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

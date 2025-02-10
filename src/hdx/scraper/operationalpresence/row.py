from typing import NamedTuple


class Row(NamedTuple):
    location_code: str
    has_hrp: str
    in_gho: str
    provider_admin1_name: str
    provider_admin2_name: str
    admin1_code: str
    admin1_name: str
    admin2_code: str
    admin2_name: str
    admin_level: int
    org_name: str
    org_acronym: str
    org_type_description: str
    sector_code: str
    sector_name: str
    reference_period_start: str
    reference_period_end: str
    dataset_id: str
    resource_id: str
    warning: str
    error: str

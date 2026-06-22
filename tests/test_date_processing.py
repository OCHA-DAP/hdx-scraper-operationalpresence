import pytest

from hdx.scraper.operationalpresence.date_processing import get_dates_from_filename


class MockResource(dict):
    pass


def country_info(use_filename_dates=True):
    return {"Filename Dates": "y" if use_filename_dates else "n"}


@pytest.mark.parametrize(
    "filename,expected_start,expected_end",
    [
        # Two-month range cases
        (
            "afghanistan-3w-operational-capacity-january-march-2026.csv",
            "01/01/2026",
            "31/03/2026",
        ),
        ("3W Burkina Faso January-February 2025", "01/01/2025", "28/02/2025"),
        (
            "cmr_3w_national_january_december_2025.xlsx",
            "01/01/2025",
            "31/12/2025",
        ),
        (
            "Ethiopia Who is Doing What Where (4W) - January - March 2026",
            "01/01/2026",
            "31/03/2026",
        ),
        ("3w_Jan - Mar 2026.xlsx", "01/01/2026", "31/03/2026"),
        (
            "who-is-doing-what-and-where_nga_3w_jul_sept_2025.xlsx",
            "01/07/2025",
            "30/09/2025",
        ),
        (
            "ss_20260526_3w_operational presence_Jan-Apr_2026.xlsx",
            "01/01/2026",
            "30/04/2026",
        ),
        ("syria-3w-presence-jan-to-jun-2025.xlsx", "01/01/2025", "30/06/2025"),
        ("presence-data-jan-aug-2025.xlsx", "01/01/2025", "31/08/2025"),
        # Two-month range with French month names
        ("BFA_3W_janvier-mars-2025.xlsx", "01/01/2025", "31/03/2025"),
        # Single-month cases (including French month names)
        ("3W_CAR_Juin 2025.xlsx", "01/06/2025", "30/06/2025"),
        ("extrait-fev-2026.xlsx", "01/02/2026", "28/02/2026"),
        ("NER_Oct_2025.csv", "01/10/2025", "31/10/2025"),
        ("TCD_3W_Dec2025.xlsx", "01/12/2025", "31/12/2025"),
    ],
)
def test_get_dates_from_filename(filename, expected_start, expected_end):
    resource = MockResource(name=filename)
    error, start, end = get_dates_from_filename(resource, country_info())
    assert not error
    assert start == expected_start
    assert end == expected_end


def test_no_country_info():
    resource = MockResource(name="some-jan-mar-2025.xlsx")
    error, start, end = get_dates_from_filename(resource, None)
    assert not error
    assert start == ""
    assert end == ""


def test_filename_dates_disabled():
    resource = MockResource(name="some-jan-mar-2025.xlsx")
    error, start, end = get_dates_from_filename(resource, country_info(False))
    assert not error
    assert start == ""
    assert end == ""


def test_no_recognizable_dates():
    resource = MockResource(name="some-random-file.xlsx")
    error, start, end = get_dates_from_filename(resource, country_info())
    assert not error
    assert start == ""
    assert end == ""

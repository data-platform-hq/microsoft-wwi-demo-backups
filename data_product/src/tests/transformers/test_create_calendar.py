import arrow
from pyspark.sql.types import Row

from data_product_microsoft_wwi.loaders.calendar import CalendarCreate
from tests.conftest import DEST_SCHEMAS


def test_calendar_check_schema(spark_session):
    cc = CalendarCreate(arrow.get('2000-01-01').date())
    calendar = cc.get_calendar_dataframe()

    assert DEST_SCHEMAS['Date'] == calendar.schema


def test_calendar_start(spark_session):
    cc = CalendarCreate(arrow.get('2000-01-01').date())
    calendar = cc.get_calendar_dataframe()

    assert calendar.agg({"Date": "min"}).collect()[0][0] == arrow.get('2000-01-01').date()


def test_calendar_structure(spark_session):
    cc = CalendarCreate(arrow.get('2000-01-01').date())
    calendar = cc.get_calendar_dataframe()
    calendar_date = calendar.where(calendar.Date == arrow.get('2000-01-01').date())

    assert 1 == calendar_date.count()

    synthetic_row: Row = calendar_date.select(
        [c for c in calendar_date.columns]).first()

    assert synthetic_row['DayNumber'] == 1
    assert synthetic_row['Day'] == 1
    assert synthetic_row['Month'] == 'January'
    assert synthetic_row['ShortMonth'] == 'Jan'
    assert synthetic_row['CalendarMonthNumber'] == 1
    assert synthetic_row['CalendarMonthLabel'] == 'CY2000-Jan'
    assert synthetic_row['CalendarYear'] == 2000
    assert synthetic_row['CalendarYearLabel'] == 'CY2000'
    assert synthetic_row['FiscalMonthNumber'] == 3
    assert synthetic_row['FiscalMonthLabel'] == 'FY2000-Jan'
    assert synthetic_row['FiscalYear'] == 2000
    assert synthetic_row['FiscalYearLabel'] == 'FY2000'
    assert synthetic_row['ISOWeekNumber'] == 52

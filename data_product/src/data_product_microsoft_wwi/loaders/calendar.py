from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, DateType

from data_product_microsoft_wwi.loaders.common import SessionInit


class CalendarCreate(SessionInit):

    def __init__(self, start_date: date):
        self._data_frame = None
        self._start_date = start_date

    def get_calendar_dataframe(self) -> DataFrame:
        if self._data_frame:
            return self._data_frame

        spark_session = self.get_spark_session()

        schema = StructType([
            StructField('start_date', DateType(), True),
            StructField('end_date', DateType(), True)
        ])

        data = [
            (self._start_date, date.today())
        ]
        dates = spark_session.createDataFrame(data, schema) \
            .selectExpr('start_date', 'CAST(ADD_MONTHS(LAST_DAY(end_date), 12) AS DATE) AS end_date')

        start_date_with_days_count = dates \
            .selectExpr('start_date', 'EXPLODE(SEQUENCE(0, DATEDIFF(end_date, start_date) - 1, 1)) AS days_shift')

        date_sequence = start_date_with_days_count.selectExpr('days_shift AS id',
                                                              'DATE_ADD(start_date, days_shift) AS full_date')

        self._data_frame = date_sequence.selectExpr(
            "full_date AS Date",
            "DAY(full_date) AS DayNumber",
            "DAY(full_date) AS Day",
            "DATE_FORMAT(full_date, 'MMMM') AS Month",
            "DATE_FORMAT(full_date, 'MMM') AS ShortMonth",
            "MONTH(full_date) AS CalendarMonthNumber",
            "CONCAT('CY', YEAR(full_date), '-', DATE_FORMAT(full_date, 'MMM')) AS CalendarMonthLabel",
            "YEAR(full_date) AS CalendarYear",
            "CONCAT('CY', YEAR(full_date)) AS CalendarYearLabel",
            '''CASE WHEN MONTH(full_date) IN (11, 12)
                THEN (MONTH(full_date) - 10)
                ELSE (MONTH(full_date) + 2)
                END AS FiscalMonthNumber''',
            '''CONCAT('FY', cast(
                CASE WHEN MONTH(full_date) IN (11, 12)
                    THEN YEAR(full_date) + 1
                    ELSE YEAR(full_date)
                    END AS string), '-', DATE_FORMAT(full_date, 'MMM')) AS FiscalMonthLabel''',
            '''CASE WHEN MONTH(full_date) IN (11, 12)
                THEN YEAR(full_date) + 1
                ELSE YEAR(full_date)
                END AS FiscalYear''',
            '''CONCAT('FY', cast(
                CASE WHEN MONTH(full_date) IN (11, 12)
                     THEN YEAR(full_date) + 1
                     ELSE YEAR(full_date)
                     END AS string)) AS FiscalYearLabel''',
            "date_part('WEEK', full_date) AS ISOWeekNumber"
        )

        return self._data_frame

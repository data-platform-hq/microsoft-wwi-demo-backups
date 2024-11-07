import datetime
import logging
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Optional, Dict

from pyspark.sql import DataFrame

from data_product_microsoft_wwi.configuration import InputTableNames, OutputDimensionsTableNames, \
    output_transformed_tables_primary_keys_names, OutputFactTableName
from data_product_microsoft_wwi.loaders import DeltaReader, SaveType
from data_product_microsoft_wwi.loaders.calendar import CalendarCreate
from data_product_microsoft_wwi.loaders.filter import DataFilter
from data_product_microsoft_wwi.loaders.readers import TablePath
from data_product_microsoft_wwi.loaders.writers import DataWriterAbstract, WriterFactory, WriterType
from data_product_microsoft_wwi.transformers import transform_employee, transform_city, transform_customer, \
    transform_stock_item, transform_transaction_type, transform_supplier, transform_payment_method, \
    transform_fact_stock_holding, transform_fact_order
from data_product_microsoft_wwi.transformers.transform_fact_sale import transform_fact_sale


class TransformerType(Enum):
    DELTA = "Delta"
    SQL = "SQL"
    SYNAPSE = "Synapse"
    SNOWFLAKE = "Snowflake"


class TransformationType(Enum):
    DIMENSIONS = "Dimensions"
    FACTS = "Facts"
    ALL = "All"


class DataProductTransformerAbstract(metaclass=ABCMeta):

    def __init__(self, reader: DeltaReader, writer: DataWriterAbstract, load_to: Optional[datetime.datetime] = None):
        self._offset_keys: Dict[TablePath, DataFrame] = {}
        self._reader = reader
        self._writer = writer
        self._load_to = load_to

    def __get_transformed_df(self, df_name: OutputDimensionsTableNames):
        if self._reader.data_frame_exists(TablePath(df_name)):
            return self._reader.get_transformed_df(TablePath(df_name))
        else:
            raise RuntimeError(f'dimension {df_name} is mandatory')

    def _fact_order_transform(self, load_to: Optional[datetime.datetime] = None):
        if self._reader.data_frame_exists(OutputFactTableName.ORDER):
            return self

        if load_to:
            data_frame_filter = DataFilter()
            date_time_since = data_frame_filter.get_since_date(self._reader, OutputFactTableName.ORDER,
                                                               'RawDataLoadDatetime')
            offset = data_frame_filter.get_offset(self._reader, OutputFactTableName.ORDER,
                                                  output_transformed_tables_primary_keys_names)
        else:
            offset = 0
            date_time_since = None
            load_to = None

        self._reader.set_data_frame(OutputFactTableName.ORDER,
                                    transform_fact_order(
                                        sales_orders=self._reader.get_source_df(
                                            InputTableNames.ORDERS),
                                        sales_orderlines=self._reader.get_source_df(
                                            InputTableNames.ORDERLINES),
                                        warehouse_package_types=self._reader.get_source_df(
                                            InputTableNames.PACKAGE_TYPES),
                                        sales_customers=self._reader.get_source_df(
                                            InputTableNames.CUSTOMERS),
                                        dimension_city=self.__get_transformed_df(
                                            OutputDimensionsTableNames.CITY),
                                        dimension_customer=self.__get_transformed_df(
                                            OutputDimensionsTableNames.CUSTOMER),
                                        dimension_stock_item=self.__get_transformed_df(
                                            OutputDimensionsTableNames.STOCK_ITEM),
                                        dimension_employee=self.__get_transformed_df(
                                            OutputDimensionsTableNames.EMPLOYEE),
                                        since=date_time_since,
                                        to=load_to,
                                        offset=offset)
                                    )

        return self

    def _fact_stock_holding_transform(self, load_to: Optional[datetime.datetime] = None):
        if self._reader.data_frame_exists(OutputFactTableName.STOCK_HOLDING):
            return self

        if load_to:
            data_frame_filter = DataFilter()
            date_time_since = data_frame_filter.get_since_date(self._reader, OutputFactTableName.STOCK_HOLDING,
                                                               'RawDataLoadDatetime')
            offset = data_frame_filter.get_offset(self._reader, OutputFactTableName.STOCK_HOLDING,
                                                  output_transformed_tables_primary_keys_names)
        else:
            offset = 0
            date_time_since = None
            load_to = None

        self._reader.set_data_frame(OutputFactTableName.STOCK_HOLDING,
                                    transform_fact_stock_holding(stock_item_holdings=self._reader.get_source_df(InputTableNames.STOCK_ITEM_HOLDINGS),
                                    dimension_stock_item=self.__get_transformed_df(OutputDimensionsTableNames.STOCK_ITEM),
                                    since=date_time_since,
                                    to=load_to,
                                    offset=offset))

        return self

    def _fact_sale_transform(self, load_to: Optional[datetime.datetime] = None):
        if self._reader.data_frame_exists(OutputFactTableName.SALE):
            return self

        if load_to:
            data_frame_filter = DataFilter()
            date_time_since = data_frame_filter.get_since_date(self._reader, OutputFactTableName.SALE,
                                                               'RawDataLoadDatetime')
            offset = data_frame_filter.get_offset(self._reader, OutputFactTableName.SALE,
                                                  output_transformed_tables_primary_keys_names)
        else:
            offset = 0
            date_time_since = None
            load_to = None

        self._reader.set_data_frame(OutputFactTableName.SALE, transform_fact_sale(
            invoices=self._reader.get_source_df(InputTableNames.INVOICES),
            invoice_lines=self._reader.get_source_df(InputTableNames.INVOICE_LINES),
            customers=self._reader.get_source_df(InputTableNames.CUSTOMERS),
            stock_items=self._reader.get_source_df(InputTableNames.STOCK_ITEMS),
            package_types=self._reader.get_source_df(InputTableNames.PACKAGE_TYPES),
            dimension_employee=self.__get_transformed_df(OutputDimensionsTableNames.EMPLOYEE),
            dimension_city=self.__get_transformed_df(OutputDimensionsTableNames.CITY),
            dimension_customer=self.__get_transformed_df(OutputDimensionsTableNames.CUSTOMER),
            dimension_stock_item=self.__get_transformed_df(OutputDimensionsTableNames.STOCK_ITEM),
            offset=offset,
            since=date_time_since,
            to=load_to
        ))

        return self

    def _city_transform(self):
        if self._reader.data_frame_exists(OutputDimensionsTableNames.CITY):
            return

        self._reader.set_data_frame(OutputDimensionsTableNames.CITY, transform_city(
            cities=self._reader.get_source_df(InputTableNames.CITIES),
            countries=self._reader.get_source_df(InputTableNames.COUNTRIES),
            state_provinces=self._reader.get_source_df(InputTableNames.STATE_PROVINCES),
            to=self._load_to,
        ))

    def _employee_transform(self):
        if self._reader.data_frame_exists(OutputDimensionsTableNames.EMPLOYEE):
            return

        self._reader.set_data_frame(OutputDimensionsTableNames.EMPLOYEE, transform_employee(
            people=self._reader.get_source_df(InputTableNames.PEOPLE),
            to=self._load_to,
        ))

    def _customer_transform(self):
        if self._reader.data_frame_exists(OutputDimensionsTableNames.CUSTOMER):
            return

        self._reader.set_data_frame(OutputDimensionsTableNames.CUSTOMER, transform_customer(
            customers=self._reader.get_source_df(InputTableNames.CUSTOMERS),
            customer_categories=self._reader.get_source_df(InputTableNames.CUSTOMER_CATEGORIES),
            buying_groups=self._reader.get_source_df(InputTableNames.BUYING_GROUPS),
            people=self._reader.get_source_df(InputTableNames.PEOPLE),
            to=self._load_to,
        ))

    def _stock_item_transform(self):
        if self._reader.data_frame_exists(OutputDimensionsTableNames.STOCK_ITEM):
            return

        self._reader.set_data_frame(OutputDimensionsTableNames.STOCK_ITEM, transform_stock_item(
            colors=self._reader.get_source_df(InputTableNames.COLORS),
            stock_groups=self._reader.get_source_df(InputTableNames.STOCK_GROUPS),
            stock_items=self._reader.get_source_df(InputTableNames.STOCK_ITEMS),
            stock_item_stock_groups=self._reader.get_source_df(InputTableNames.STOCK_ITEM_STOCK_GROUPS),
            package_types=self._reader.get_source_df(InputTableNames.PACKAGE_TYPES),
            to=self._load_to,
        ))

    def _transaction_types_transform(self):
        if self._reader.data_frame_exists(OutputDimensionsTableNames.TRANSACTION_TYPE):
            return

        self._reader.set_data_frame(OutputDimensionsTableNames.TRANSACTION_TYPE, transform_transaction_type(
            transactions_types=self._reader.get_source_df(InputTableNames.TRANSACTION_TYPES),
            to=self._load_to,
        ))

    def _supplier_transform(self):
        if self._reader.data_frame_exists(OutputDimensionsTableNames.SUPPLIER):
            return

        self._reader.set_data_frame(OutputDimensionsTableNames.SUPPLIER, transform_supplier(
            suppliers=self._reader.get_source_df(InputTableNames.SUPPLIERS),
            supplier_categories=self._reader.get_source_df(InputTableNames.SUPPLIER_CATEGORIES),
            people=self._reader.get_source_df(InputTableNames.PEOPLE),
            to=self._load_to,
        ))

    def _payment_method_transform(self):
        if self._reader.data_frame_exists(OutputDimensionsTableNames.PAYMENT_METHOD):
            return

        self._reader.set_data_frame(OutputDimensionsTableNames.PAYMENT_METHOD, transform_payment_method(
            payment_method=self._reader.get_source_df(InputTableNames.PAYMENT_METHODS),
            to=self._load_to,
        ))

    def save(self, save_type: SaveType, save_df_list: list):
        for df_name in save_df_list:
            df = self._reader.get_transformed_df(df_name)
            if df:
                self._writer.write(df, table_name=df_name, save_mode=save_type)
            else:
                logging.warning(f"Cant get {df_name} DataFrame for saving")

    def transform(self, transformation_type: TransformationType = TransformationType.ALL,
                  load_to: Optional[datetime.datetime] = None):

        if transformation_type == TransformationType.DIMENSIONS:
            self._dimension_transform()

        elif transformation_type == TransformationType.FACTS or transformation_type == TransformationType.ALL:
            self._dimension_transform()
            self._fact_sale_transform(load_to)
            self._fact_stock_holding_transform(load_to)
            self._fact_order_transform(load_to)

        else:
            logging.warning('transformation_type has not been identified')

    @abstractmethod
    def _dimension_transform(self):
        ...


class TransformerFactory:
    def __init__(self, load_to: Optional[datetime.datetime] = None):
        self._load_to = load_to

    def get_transformer(self, transformer_type: TransformerType, **kwargs) -> DataProductTransformerAbstract:

        delta_reader = DeltaReader(kwargs["source_catalog"], kwargs["source_schema"],
                                   kwargs.get("catalog"), kwargs.get("schema"))
        calendar = CalendarCreate(kwargs["start_date"])

        if transformer_type == TransformerType.DELTA:
            writer = WriterFactory.get_writer(WriterType.DELTA, **kwargs)

        elif transformer_type == TransformerType.SQL:
            writer = WriterFactory.get_writer(WriterType.SQL, **kwargs)

        elif transformer_type == TransformerType.SYNAPSE:
            writer = WriterFactory.get_writer(WriterType.SYNAPSE, **kwargs)

        elif transformer_type == TransformerType.SNOWFLAKE:
            writer = WriterFactory.get_writer(WriterType.SNOWFLAKE, **kwargs)
        else:
            raise NotImplementedError(f"Transformer type {transformer_type} is not supported")

        return Transformer(delta_reader, writer, self._load_to, calendar)


class Transformer(DataProductTransformerAbstract):
    def __init__(self, reader: DeltaReader, writer: DataWriterAbstract, load_to: Optional[datetime.datetime] = None,
                 calendar: Optional[CalendarCreate] = None):
        super().__init__(reader, writer, load_to)
        self._calendar = calendar

    def _dimension_transform(self):
        self._city_transform()
        self._employee_transform()
        self._customer_transform()
        self._stock_item_transform()
        self._transaction_types_transform()
        self._supplier_transform()
        self._payment_method_transform()

        if self._calendar is not None:
            self._reader.set_data_frame(OutputDimensionsTableNames.CALENDAR, self._calendar.get_calendar_dataframe())

        return self

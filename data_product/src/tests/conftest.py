import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, \
    LongType, DateType, DecimalType, DoubleType

from data_product_microsoft_wwi.configuration import ResourcesConfigs
from helpers import local_spark_session_init

fixtures_path = Path(__file__).parent / 'fixtures'

SRS_SCHEMAS = {
    'People': StructType([
        StructField("PersonID", IntegerType(), False),
        StructField("FullName", StringType(), True),
        StructField("PreferredName", StringType(), True),
        StructField("SearchName", StringType(), True),
        StructField("IsPermittedToLogon", BooleanType(), True),
        StructField("LogonName", StringType(), True),
        StructField("IsExternalLogonProvider", BooleanType(), True),
        StructField("HashedPassword", StringType(), True),
        StructField("IsSystemUser", BooleanType(), True),
        StructField("IsEmployee", BooleanType(), True),
        StructField("IsSalesperson", BooleanType(), True),
        StructField("UserPreferences", StringType(), True),
        StructField("PhoneNumber", StringType(), True),
        StructField("FaxNumber", StringType(), True),
        StructField("EmailAddress", StringType(), True),
        StructField("Photo", StringType(), True),
        StructField("CustomFields", StringType(), True),
        StructField("OtherLanguages", StringType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),
    'Cities': StructType([
        StructField("CityID", IntegerType(), False),
        StructField("CityName", StringType(), True),
        StructField("StateProvinceID", IntegerType(), True),
        StructField("Location", StringType(), True),
        StructField("LatestRecordedPopulation", LongType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),
    'Countries': StructType([
        StructField("CountryID", IntegerType(), False),
        StructField("CountryName", StringType(), True),
        StructField("FormalName", IntegerType(), True),
        StructField("IsoAlpha3Code", StringType(), True),
        StructField("IsoNumericCode", IntegerType(), True),
        StructField("CountryType", StringType(), True),
        StructField("LatestRecordedPopulation", LongType(), True),
        StructField("Continent", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("Subregion", StringType(), True),
        StructField("Border", StringType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),
    'StateProvinces': StructType([
        StructField("StateProvinceID", IntegerType(), False),
        StructField("StateProvinceCode", StringType(), True),
        StructField("StateProvinceName", StringType(), True),
        StructField("CountryID", IntegerType(), True),
        StructField("SalesTerritory", StringType(), True),
        StructField("Border", StringType(), True),
        StructField("LatestRecordedPopulation", LongType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),
    'Invoices': StructType([
        StructField("InvoiceID", IntegerType(), False),
        StructField("CustomerID", IntegerType(), True),
        StructField("BillToCustomerID", IntegerType(), True),
        StructField("OrderID", IntegerType(), True),
        StructField("DeliveryMethodID", IntegerType(), True),
        StructField("ContactPersonID", IntegerType(), True),
        StructField("AccountsPersonID", IntegerType(), True),
        StructField("SalespersonPersonID", IntegerType(), True),
        StructField("PackedByPersonID", IntegerType(), True),
        StructField("InvoiceDate", DateType(), True),
        StructField("CustomerPurchaseOrderNumber", StringType(), True),
        StructField("IsCreditNote", BooleanType(), True),
        StructField("CreditNoteReason", StringType(), True),
        StructField("Comments", StringType(), True),
        StructField("DeliveryInstructions", StringType(), True),
        StructField("InternalComments", StringType(), True),
        StructField("TotalDryItems", IntegerType(), True),
        StructField("TotalChillerItems", IntegerType(), True),
        StructField("DeliveryRun", StringType(), True),
        StructField("RunPosition", StringType(), True),
        StructField("ReturnedDeliveryData", StringType(), True),
        StructField("ConfirmedDeliveryTime", TimestampType(), True),
        StructField("ConfirmedReceivedBy", StringType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("LastEditedWhen", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),
    'InvoiceLines': StructType([
        StructField("InvoiceLineID", IntegerType(), False),
        StructField("InvoiceID", IntegerType(), True),
        StructField("StockItemID", IntegerType(), True),
        StructField("Description", StringType(), True),
        StructField("PackageTypeID", IntegerType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", DecimalType(18, 2), True),
        StructField("TaxRate", DecimalType(18, 3), True),
        StructField("TaxAmount", DecimalType(18, 2), True),
        StructField("LineProfit", DecimalType(18, 2), True),
        StructField("ExtendedPrice", DecimalType(18, 2), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("LastEditedWhen", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),

    'Customers': StructType([
        StructField("CustomerID", IntegerType(), False),
        StructField("CustomerName", StringType(), True),
        StructField("BillToCustomerID", IntegerType(), True),
        StructField("CustomerCategoryID", IntegerType(), True),
        StructField("BuyingGroupID", IntegerType(), True),
        StructField("PrimaryContactPersonID", IntegerType(), True),
        StructField("AlternateContactPersonID", IntegerType(), True),
        StructField("DeliveryMethodID", IntegerType(), True),
        StructField("DeliveryCityID", IntegerType(), True),
        StructField("PostalCityID", IntegerType(), True),
        StructField("CreditLimit", DecimalType(18, 2), True),
        StructField("AccountOpenedDate", DateType(), True),
        StructField("StandardDiscountPercentage", DecimalType(18, 3), True),
        StructField("IsStatementSent", BooleanType(), True),
        StructField("IsOnCreditHold", BooleanType(), True),
        StructField("PaymentDays", IntegerType(), True),
        StructField("PhoneNumber", StringType(), True),
        StructField("FaxNumber", StringType(), True),
        StructField("DeliveryRun", StringType(), False),
        StructField("RunPosition", StringType(), True),
        StructField("WebsiteURL", StringType(), True),
        StructField("DeliveryAddressLine1", StringType(), True),
        StructField("DeliveryAddressLine2", StringType(), True),
        StructField("DeliveryPostalCode", StringType(), True),
        StructField("DeliveryLocation", StringType(), True),
        StructField("PostalAddressLine1", StringType(), True),
        StructField("PostalAddressLine2", StringType(), True),
        StructField("PostalPostalCode", StringType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),

    'CustomerCategories': StructType([
        StructField("CustomerCategoryID", IntegerType(), False),
        StructField("CustomerCategoryName", StringType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),
    'BuyingGroups': StructType([
        StructField("BuyingGroupID", IntegerType(), False),
        StructField("BuyingGroupName", StringType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),
    'PackageTypes': StructType([
        StructField("PackageTypeID", IntegerType(), False),
        StructField("PackageTypeName", StringType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),
    'Colors': StructType([
        StructField("ColorID", IntegerType(), False),
        StructField("ColorName", StringType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),

    'StockGroups': StructType([
        StructField("StockGroupID", IntegerType(), False),
        StructField("StockGroupName", StringType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True),
    ]),

    'StockItems': StructType([
        StructField("StockItemID", IntegerType(), False),
        StructField("StockItemName", StringType(), False),
        StructField("SupplierID", IntegerType(), False),
        StructField("ColorID", IntegerType(), True),
        StructField("UnitPackageID", IntegerType(), False),
        StructField("OuterPackageID", IntegerType(), False),
        StructField("Brand", StringType(), True),
        StructField("Size", StringType(), True),
        StructField("LeadTimeDays", IntegerType(), False),
        StructField("QuantityPerOuter", IntegerType(), False),
        StructField("IsChillerStock", BooleanType(), False),
        StructField("Barcode", StringType(), True),
        StructField("TaxRate", DecimalType(18, 3), False),
        StructField("UnitPrice", DecimalType(18, 2), False),
        StructField("RecommendedRetailPrice", DecimalType(18, 2), True),
        StructField("TypicalWeightPerUnit", DecimalType(18, 3), False),
        StructField("MarketingComments", StringType(), True),
        StructField("InternalComments", StringType(), True),
        StructField("Photo", StringType(), True),
        StructField("CustomFields", StringType(), True),
        StructField("Tags", StringType(), True),
        StructField("SearchDetails", StringType(), False),
        StructField("LastEditedBy", IntegerType(), False),
        StructField("ValidFrom", TimestampType(), False),
        StructField("ValidTo", TimestampType(), False),
        StructField("LoadDatetime", TimestampType(), True),
    ]),

    'StockItemStockGroups': StructType([
        StructField("StockItemStockGroupID", IntegerType(), False),
        StructField("StockItemID", IntegerType(), False),
        StructField("StockGroupID", IntegerType(), False),
        StructField("LastEditedBy", IntegerType(), False),
        StructField("LastEditedWhen", TimestampType(), False),
        StructField("LoadDatetime", TimestampType(), True),
    ]),

    "SalesOrders": StructType([
        StructField("OrderID", IntegerType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("SalespersonPersonID", IntegerType(), True),
        StructField("PickedByPersonID", IntegerType(), True),
        StructField("ContactPersonID", IntegerType(), True),
        StructField("BackorderOrderID", IntegerType(), True),
        StructField("OrderDate", DateType(), True),
        StructField("ExpectedDeliveryDate", DateType(), True),
        StructField("CustomerPurchaseOrderNumber", StringType(), True),
        StructField("IsUndersupplyBackordered", BooleanType(), True),
        StructField("Comments", StringType(), True),
        StructField("DeliveryInstructions", StringType(), True),
        StructField("InternalComments", StringType(), True),
        StructField("PickingCompletedWhen", TimestampType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("LastEditedWhen", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True)
    ]),

    "SalesOrderLines": StructType([
        StructField("OrderLineID", IntegerType(), True),
        StructField("OrderID", IntegerType(), True),
        StructField("StockItemID", IntegerType(), True),
        StructField("Description", StringType(), True),
        StructField("PackageTypeID", IntegerType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", DecimalType(18, 2), True),
        StructField("TaxRate", DecimalType(18, 3), True),
        StructField("PickedQuantity", IntegerType(), True),
        StructField("PickingCompletedWhen", TimestampType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("LastEditedWhen", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True)
    ]),

    "WarehousePackageTypes": StructType([
        StructField("PackageTypeID", IntegerType(), True),
        StructField("PackageTypeName", StringType(), True),
        StructField("LastEditedBy", IntegerType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True)
    ]),

    "Suppliers": StructType([
        StructField("SupplierID", IntegerType(), True),
        StructField("SupplierName", StringType(), True),
        StructField("SupplierCategoryID", IntegerType(), True),
        StructField("PrimaryContactPersonID", IntegerType(), True),
        StructField("SupplierReference", StringType(), True),
        StructField("PaymentDays", IntegerType(), True),
        StructField("DeliveryPostalCode", StringType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True)
    ]),

    "SupplierCategories": StructType([
        StructField("SupplierCategoryID", IntegerType(), True),
        StructField("SupplierCategoryName", StringType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LoadDatetime", TimestampType(), True)
    ])
}

DEST_SCHEMAS = {
    'Employee': StructType([
        StructField("EmployeeKey", IntegerType(), False),
        StructField("WWIEmployeeID", IntegerType(), True),
        StructField("Employee", StringType(), True),
        StructField("PreferredName", StringType(), True),
        StructField("IsSalesperson", BooleanType(), True),
        StructField("Photo", StringType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LineageKey", IntegerType(), False)
    ]),
    'City': StructType([
        StructField("CityKey", IntegerType(), False),
        StructField("WWICityID", IntegerType(), True),
        StructField("City", StringType(), True),
        StructField("StateProvince", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Continent", StringType(), True),
        StructField("SalesTerritory", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("Subregion", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("LatestRecordedPopulation", LongType(), False),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LineageKey", IntegerType(), False),
        StructField("Longitude", DoubleType(), True),
        StructField("Latitude", DoubleType(), True),

    ]),

    'Sale': StructType([
        StructField("SaleKey", IntegerType(), False),
        StructField("CityKey", IntegerType(), False),
        StructField("CustomerKey", IntegerType(), False),
        StructField("BillToCustomerKey", IntegerType(), False),
        StructField("StockItemKey", IntegerType(), False),
        StructField("InvoiceDateKey", DateType(), True),
        StructField("DeliveryDateKey", DateType(), True),
        StructField("SalespersonKey", IntegerType(), False),
        StructField("WWIInvoiceID", IntegerType(), True),
        StructField("Description", StringType(), True),
        StructField("Package", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", DecimalType(18, 2), True),
        StructField("TaxRate", DecimalType(18, 3), True),
        StructField("TotalExcludingTax", DecimalType(19, 2), True),
        StructField("TaxAmount", DecimalType(18, 2), True),
        StructField("Profit", DecimalType(18, 2), True),
        StructField("TotalIncludingTax", DecimalType(18, 2), True),
        StructField("TotalDryItems", IntegerType(), True),
        StructField("TotalChillerItems", IntegerType(), True),
        StructField("LineageKey", IntegerType(), False),
        StructField('RawDataLoadDatetime', TimestampType(), True)
    ]),

    'Date': StructType([
        StructField("Date", DateType(), True),
        StructField("DayNumber", IntegerType(), True),
        StructField("Day", IntegerType(), True),
        StructField("Month", StringType(), True),
        StructField("ShortMonth", StringType(), True),
        StructField("CalendarMonthNumber", IntegerType(), True),
        StructField("CalendarMonthLabel", StringType(), True),
        StructField("CalendarYear", IntegerType(), True),
        StructField("CalendarYearLabel", StringType(), True),
        StructField("FiscalMonthNumber", IntegerType(), True),
        StructField("FiscalMonthLabel", StringType(), True),
        StructField("FiscalYear", IntegerType(), True),
        StructField("FiscalYearLabel", StringType(), True),
        StructField("ISOWeekNumber", IntegerType(), True)
    ]),

    'Customer': StructType([
        StructField("CustomerKey", IntegerType(), False),
        StructField("WWICustomerID", IntegerType(), True),
        StructField("Customer", StringType(), True),
        StructField("BillToCustomer", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("BuyingGroup", StringType(), True),
        StructField("PrimaryContact", StringType(), True),
        StructField("PostalCode", StringType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LineageKey", IntegerType(), False)
    ]),

    'StockItem': StructType([
        StructField("StockItemKey", IntegerType(), False),
        StructField("WWIStockItemID", IntegerType(), True),
        StructField("StockItem", StringType(), True),
        StructField("Color", StringType(), False),
        StructField("SellingPackage", StringType(), True),
        StructField("BuyingPackage", StringType(), True),
        StructField("Brand", StringType(), False),
        StructField("Size", StringType(), False),
        StructField("LeadTimeDays", IntegerType(), True),
        StructField("QuantityPerOuter", IntegerType(), True),
        StructField("IsChillerStock", BooleanType(), True),
        StructField("Barcode", StringType(), False),
        StructField("TaxRate", DecimalType(18, 3), True),
        StructField("UnitPrice", DecimalType(18, 2), True),
        StructField("RecommendedRetailPrice", DecimalType(18, 2), True),
        StructField("TypicalWeightPerUnit", DecimalType(18, 3), True),
        StructField("Photo", StringType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
        StructField("LineageKey", IntegerType(), False),
    ]),

    "FactOrder": StructType([
        StructField("OrderKey", IntegerType(), False),
        StructField("CityKey", IntegerType(), False),
        StructField("CustomerKey", IntegerType(), False),
        StructField("StockItemKey", IntegerType(), False),
        StructField("OrderDateKey", DateType(), True),
        StructField("PickedDateKey", DateType(), True),
        StructField("SalespersonKey", IntegerType(), False),
        StructField("PickerKey", IntegerType(), False),
        StructField("WWIOrderID", IntegerType(), True),
        StructField("WWIBackorderID", IntegerType(), True),
        StructField("Description", StringType(), True),
        StructField("Package", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", DecimalType(18, 2), True),
        StructField("TaxRate", DecimalType(18, 3), True),
        StructField("TotalExcludingTax", DecimalType(30, 2), True),
        StructField("TaxAmount", DecimalType(35, 2), True),
        StructField("TotalIncludingTax", DecimalType(36, 2), True),
        StructField("LineageKey", IntegerType(), False),
        StructField("RawDataLoadDatetime", TimestampType(), True)
    ]),

}


@pytest.fixture(scope='session')
def spark_session() -> SparkSession:
    return local_spark_session_init()


@pytest.fixture(scope='module')
def tsv_loader(request, spark_session):
    return (spark_session.read.format("csv")
            .option('header', True)
            .option('sep', '\t')
            .load(f'{fixtures_path}/{request.param[0]}', schema=request.param[1]))


@pytest.fixture(scope='session')
def config_loading(spark_session):
    return ResourcesConfigs(
        db_url=f'jdbc:sqlserver://{os.getenv("MSSQL_URL")}'
               f':{os.getenv("MSSQL_PORT")};'
               f'database={os.getenv("MSSQL_DB_NAME")};'
               f'user={os.getenv("MSSQL_USER_NAME")};'
               f'password={os.getenv("MSSQL_PASSWORD")}',
        input_delta_tables_path=os.getenv("DELTA_TABLES_SRS"),
        output_delta_tables_path=os.getenv("DELTA_TABLES_DEST"),
        databricks_schema="data_product_microsoft_wwi"
    )

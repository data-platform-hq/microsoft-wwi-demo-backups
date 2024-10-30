from enum import Enum


class ResourcesConfigs:
    """
    Set mandatory paths to data resources
    """

    def __init__(self,
                 db_url: str,
                 input_delta_tables_path: str,
                 output_delta_tables_path: str,
                 databricks_schema: str):
        self.DB_URL = db_url
        self.INPUT_DELTA_TABLES_PATH = input_delta_tables_path
        self.OUTPUT_DELTA_TABLES_PATH = output_delta_tables_path
        self.DATABRICKS_SCHEMA = databricks_schema


class InputTableNames(Enum):
    BUYING_GROUPS = "sales_buyinggroups"
    CITIES = "application_cities"
    COLORS = "warehouse_colors"
    COUNTRIES = "application_countries"
    CUSTOMERS = "sales_customers"
    CUSTOMER_CATEGORIES = "sales_customercategories"
    INVOICES = "sales_invoices"
    INVOICE_LINES = "sales_invoicelines"
    PACKAGE_TYPES = "warehouse_packagetypes"
    PEOPLE = "application_people"
    STATE_PROVINCES = "application_stateprovinces"
    STOCK_GROUPS = "warehouse_stockgroups"
    STOCK_ITEMS = "warehouse_stockitems"
    STOCK_ITEM_STOCK_GROUPS = "warehouse_stockitemstockgroups"
    TRANSACTION_TYPES = "application_transactiontypes"
    SUPPLIERS = "purchasing_suppliers"
    SUPPLIER_CATEGORIES = "purchasing_suppliercategories"
    PAYMENT_METHODS = "application_paymentmethods"
    STOCK_ITEM_HOLDINGS = "warehouse_stockitemholdings"
    ORDERS = "sales_orders"
    ORDERLINES = "sales_orderlines"


input_table_name_path = {
    InputTableNames.BUYING_GROUPS: "Sales",
    InputTableNames.CITIES: "Application",
    InputTableNames.COLORS: "Warehouse",
    InputTableNames.COUNTRIES: "Application",
    InputTableNames.CUSTOMERS: "Sales",
    InputTableNames.CUSTOMER_CATEGORIES: "Sales",
    InputTableNames.INVOICES: "Sales",
    InputTableNames.INVOICE_LINES: "Sales",
    InputTableNames.PACKAGE_TYPES: "Warehouse",
    InputTableNames.PEOPLE: "Application",
    InputTableNames.STATE_PROVINCES: "Application",
    InputTableNames.STOCK_GROUPS: "Warehouse",
    InputTableNames.STOCK_ITEMS: "Warehouse",
    InputTableNames.STOCK_ITEM_STOCK_GROUPS: "Warehouse",
    InputTableNames.TRANSACTION_TYPES: "Application",
    InputTableNames.SUPPLIERS: "Purchasing",
    InputTableNames.SUPPLIER_CATEGORIES: "Purchasing",
    InputTableNames.PAYMENT_METHODS: "Application",
    InputTableNames.STOCK_ITEM_HOLDINGS: "Warehouse",
    InputTableNames.ORDERS: "Sales",
    InputTableNames.ORDERLINES: "Sales",
}


class OutputDimensionsTableNames(Enum):
    CALENDAR = "dimension_date"
    CITY = "dimension_city"
    CUSTOMER = "dimension_customer"
    EMPLOYEE = "dimension_employee"
    STOCK_ITEM = "dimension_stockitem"
    TRANSACTION_TYPE = "dimension_transactiontype"
    SUPPLIER = "dimension_supplier"
    PAYMENT_METHOD = "dimension_paymentmethod"


class OutputFactTableName(Enum):
    SALE = "fact_sale"
    STOCK_HOLDING = "fact_stockholding"
    ORDER = "fact_order"

output_transformed_tables_primary_keys_names = {
    OutputDimensionsTableNames.EMPLOYEE: 'EmployeeKey',
    OutputDimensionsTableNames.CITY: 'CityKey',
    OutputDimensionsTableNames.CUSTOMER: 'CustomerKey',
    OutputDimensionsTableNames.STOCK_ITEM: 'StockItemKey',
    OutputDimensionsTableNames.TRANSACTION_TYPE: 'TransactionTypeKey',
    OutputDimensionsTableNames.SUPPLIER: 'SupplierKey',
    OutputFactTableName.SALE: 'SaleKey',
    OutputDimensionsTableNames.PAYMENT_METHOD: 'PaymentMethodKey',
    OutputFactTableName.STOCK_HOLDING: 'StockHoldingKey',
    OutputFactTableName.ORDER: 'OrderKey',
}

merge_keys_names = {
    OutputDimensionsTableNames.CITY: ['WWICityID', 'ValidFrom'],
    OutputDimensionsTableNames.CUSTOMER: ['WWICustomerID', 'ValidFrom'],
    OutputDimensionsTableNames.EMPLOYEE: ['WWIEmployeeID', 'ValidFrom'],
    OutputFactTableName.SALE: ['StockItemKey', 'WWIInvoiceID'],
    OutputDimensionsTableNames.STOCK_ITEM: ['WWIStockItemID', 'ValidFrom'],
    OutputDimensionsTableNames.PAYMENT_METHOD: ['WWIPaymentMethodID', 'ValidFrom'],
    OutputFactTableName.STOCK_HOLDING: ['StockItemKey'],
    OutputFactTableName.ORDER: ['StockItemKey', 'WWIOrderID'],
    OutputDimensionsTableNames.TRANSACTION_TYPE: ['TransactionTypeKey', 'ValidFrom'],
    OutputDimensionsTableNames.SUPPLIER: ['SupplierKey', 'ValidFrom'],
}

output_table_name_path = {
    OutputDimensionsTableNames.CALENDAR: "Dimension",
    OutputDimensionsTableNames.CITY: "Dimension",
    OutputDimensionsTableNames.CUSTOMER: "Dimension",
    OutputDimensionsTableNames.EMPLOYEE: "Dimension",
    OutputFactTableName.SALE: "Fact",
    OutputDimensionsTableNames.STOCK_ITEM: "Dimension",
    OutputDimensionsTableNames.TRANSACTION_TYPE: "Dimension",
    OutputDimensionsTableNames.SUPPLIER: "Dimension",
    OutputDimensionsTableNames.PAYMENT_METHOD: "Dimension",
    OutputFactTableName.STOCK_HOLDING: "Fact",
    OutputFactTableName.ORDER: "Fact",
}

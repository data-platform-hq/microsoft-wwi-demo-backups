import pytest
from datetime import datetime

from data_product_microsoft_wwi.transformers import transform_supplier
from tests.conftest import SRS_SCHEMAS


@pytest.fixture(scope="session")
def suppliers_df(spark_session):
    data_suppliers = \
        [(1, "Supplier1", 1, 1, "Ref1", 30, "12345", datetime(2021, 1, 1), datetime(2022, 1, 1), datetime(2021, 1, 1))]
    schema_suppliers = SRS_SCHEMAS["Suppliers"]
    return spark_session.createDataFrame(data_suppliers, schema_suppliers)


@pytest.fixture(scope="session")
def supplier_categories_df(spark_session):
    data_supplier_categories = [(1, "Category 1", datetime(2021, 1, 1), datetime(2022, 1, 1), datetime(2021, 1, 1))]
    schema_supplier_categories = SRS_SCHEMAS["SupplierCategories"]
    return spark_session.createDataFrame(data_supplier_categories, schema_supplier_categories)


@pytest.fixture(scope="session")
def people_df(spark_session):
    data_people = [(
        1, "John Doe", "John", "Johndoe", True, "john.doe",
        False, "hashed_password", True, True, True, "User Preferences",
        "123-456-7890", "123-456-7891", "john.doe@example.com", "", "{'A':1}", "OtherLanguages",
        1, datetime(2021, 1, 1), datetime(2022, 1, 1), datetime(2021, 1, 1)
    )]
    schema_people = SRS_SCHEMAS["People"]
    return spark_session.createDataFrame(data_people, schema_people)


def test_supplier_transformed(suppliers_df, supplier_categories_df, people_df):
    load_to = datetime(2025, 1, 1)
    supplier_df = transform_supplier(
                suppliers=suppliers_df,
                supplier_categories=supplier_categories_df,
                people=people_df,
                to=load_to,
    )

    row = supplier_df.collect()[1]
    assert row["SupplierKey"] == 1
    assert row["WWISupplierID"] == 1
    assert row["Supplier"] == "Supplier1"
    assert row["Category"] == "Category 1"
    assert row["PrimaryContact"] == "John Doe"
    assert row["SupplierReference"] == "Ref1"
    assert row["PaymentDays"] == 30
    assert row["PostalCode"] == "12345"
    assert row["ValidFrom"] == datetime(2021, 1, 1)
    assert row["ValidTo"] == datetime(2022, 1, 1)
    assert row["LineageKey"] == 6

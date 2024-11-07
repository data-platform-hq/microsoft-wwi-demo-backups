import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from data_product_microsoft_wwi.transformers.config import VALID_FROM, VALID_TO
from data_product_microsoft_wwi.transformers.id_generator import zip_with_row_number_as_key


def transform_customer(customers: DataFrame,
                       customer_categories: DataFrame,
                       buying_groups: DataFrame,
                       people: DataFrame,
                       to: Optional[datetime.datetime] = None,
                       offset: int = 0,
                       primary_key_name: str = 'CustomerKey') -> DataFrame:
    initial_load_date = F.to_timestamp(F.lit(VALID_FROM), "yyyy-MM-dd HH:mm:ss.SSSSSSS")
    end_of_time = F.to_timestamp(F.lit(VALID_TO), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')

    changed_buying_groups = (
        buying_groups.alias('bg')
        .filter(buying_groups.ValidFrom != initial_load_date)
        .join(
            customers.alias("c"),
            [buying_groups.BuyingGroupID == F.col("c.BuyingGroupID"),
             F.col("c.ValidFrom") <= buying_groups.ValidFrom,
             buying_groups.ValidFrom < F.col("c.ValidTo")],
            "inner")
        .join(
            customers.alias("bt"),
            [F.col("c.BillToCustomerID") == F.col("bt.CustomerID"),
             F.col("bt.ValidFrom") <= buying_groups.ValidFrom,
             buying_groups.ValidFrom < F.col("bt.ValidTo")],
            "inner")
        .join(
            customer_categories,
            [customer_categories.CustomerCategoryID == F.col("c.CustomerCategoryID"),
             customer_categories.ValidFrom <= buying_groups.ValidFrom,
             buying_groups.ValidFrom < customer_categories.ValidTo],
            "inner")
        .join(
            people,
            [people.PersonID == F.col("c.PrimaryContactPersonID"),
             people.ValidFrom <= buying_groups.ValidFrom,
             buying_groups.ValidFrom < people.ValidTo],
            "inner")
        .select(
            F.col("c.CustomerID").alias("WWICustomerID"),
            F.col("c.CustomerName").alias("Customer"),
            F.col("bt.CustomerName").alias("BillToCustomer"),
            customer_categories.CustomerCategoryName.alias("Category"),
            buying_groups.BuyingGroupName.alias("BuyingGroup"),
            people.FullName.alias("PrimaryContact"),
            F.col("c.DeliveryPostalCode").alias("PostalCode"),
            F.col("bg.ValidFrom"),
            F.col("bg.ValidTo"),
            F.col("bg.LoadDatetime")

        )
    )

    changed_people = (
        people.alias("p")
        .filter(people.ValidFrom != initial_load_date)
        .filter(people.ValidFrom != people.ValidTo)
        .join(
            customers.alias("c"),
            [people.PersonID == F.col("c.PrimaryContactPersonID"),
             F.col("c.ValidFrom") <= people.ValidFrom,
             people.ValidFrom < F.col("c.ValidTo")],
            "inner")
        .join(
            customers.alias("bt"),
            [F.col("c.BillToCustomerID") == F.col("bt.CustomerID"),
             F.col("bt.ValidFrom") <= people.ValidFrom,
             people.ValidFrom < F.col("bt.ValidTo")],
            "inner")
        .join(
            buying_groups,
            [buying_groups.BuyingGroupID == F.col("c.BuyingGroupID"),
             buying_groups.ValidFrom <= people.ValidFrom,
             people.ValidFrom < buying_groups.ValidTo],
            "inner")
        .join(
            customer_categories,
            [customer_categories.CustomerCategoryID == F.col("c.CustomerCategoryID"),
             customer_categories.ValidFrom <= people.ValidFrom,
             people.ValidFrom < customer_categories.ValidTo],
            "inner")
        .select(
            F.col("c.CustomerID").alias("WWICustomerID"),
            F.col("c.CustomerName").alias("Customer"),
            F.col("bt.CustomerName").alias("BillToCustomer"),
            customer_categories.CustomerCategoryName.alias("Category"),
            buying_groups.BuyingGroupName.alias("BuyingGroup"),
            people.FullName.alias("PrimaryContact"),
            F.col("c.DeliveryPostalCode").alias("PostalCode"),
            F.col("p.ValidFrom"),
            F.col("p.ValidTo"),
            F.col("p.LoadDatetime")
        )
    )

    changed_customer_categories = (
        customer_categories.alias('cc')
        .filter(customer_categories.ValidFrom != initial_load_date)
        .join(
            customers.alias("c"),
            [customer_categories.CustomerCategoryID == F.col("c.CustomerCategoryID"),
             F.col("c.ValidFrom") <= customer_categories.ValidFrom,
             customer_categories.ValidFrom < F.col("c.ValidTo")],
            "inner")
        .join(
            customers.alias("bt"),
            [F.col("c.BillToCustomerID") == F.col("bt.CustomerID"),
             F.col("bt.ValidFrom") <= customer_categories.ValidFrom,
             customer_categories.ValidFrom < F.col("bt.ValidTo")],
            "inner")
        .join(
            buying_groups,
            [buying_groups.BuyingGroupID == F.col("c.BuyingGroupID"),
             buying_groups.ValidFrom <= customer_categories.ValidFrom,
             customer_categories.ValidFrom < buying_groups.ValidTo],
            "inner")
        .join(
            people,
            [people.PersonID == F.col("c.PrimaryContactPersonID"),
             people.ValidFrom <= customer_categories.ValidFrom,
             customer_categories.ValidFrom < people.ValidTo],
            "inner")
        .select(
            F.col("c.CustomerID").alias("WWICustomerID"),
            F.col("c.CustomerName").alias("Customer"),
            F.col("bt.CustomerName").alias("BillToCustomer"),
            customer_categories.CustomerCategoryName.alias("Category"),
            buying_groups.BuyingGroupName.alias("BuyingGroup"),
            people.FullName.alias("PrimaryContact"),
            F.col("c.DeliveryPostalCode").alias("PostalCode"),
            F.col("cc.ValidFrom"),
            F.col("cc.ValidTo"),
            F.col("cc.LoadDatetime")
        )
    )

    changed_customers = (
        customers.alias("c")
        .join(
            buying_groups,
            [buying_groups.BuyingGroupID == F.col("c.BuyingGroupID"),
             buying_groups.ValidFrom <= F.col("c.ValidFrom"),
             F.col("c.ValidFrom") < buying_groups.ValidTo
             ],
            "inner")
        .join(
            customer_categories,
            [customer_categories.CustomerCategoryID == F.col("c.CustomerCategoryID"),
             customer_categories.ValidFrom <= F.col("c.ValidFrom"),
             F.col("c.ValidFrom") < customer_categories.ValidTo
             ],
            "inner")
        .join(
            customers.alias("bt"),
            [F.col("c.BillToCustomerID") == F.col("bt.CustomerID"),
             F.col("bt.ValidFrom") <= F.col("c.ValidFrom"),
             F.col("c.ValidFrom") < F.col("bt.ValidTo")
             ],
            "inner")
        .join(
            people,
            [people.PersonID == F.col("c.PrimaryContactPersonID"),
             people.ValidFrom <= F.col("c.ValidFrom"),
             F.col("c.ValidFrom") < people.ValidTo
             ],
            "inner")
        .select(
            F.col("c.CustomerID").alias("WWICustomerID"),
            F.col("c.CustomerName").alias("Customer"),
            F.col("bt.CustomerName").alias("BillToCustomer"),
            customer_categories.CustomerCategoryName.alias("Category"),
            buying_groups.BuyingGroupName.alias("BuyingGroup"),
            people.FullName.alias("PrimaryContact"),
            F.col("c.DeliveryPostalCode").alias("PostalCode"),
            F.col("c.ValidFrom"),
            F.col("c.ValidTo"),
            F.col("c.LoadDatetime")

        )
    )

    customers_staging = (
        changed_customers.union(changed_customer_categories).union(changed_buying_groups).union(changed_people)
        .withColumn("RawValidTo", F.col("ValidTo"))
        .withColumn("EndOfTime", end_of_time)
        .withColumn("DataProductLoadDatetime", F.to_timestamp(F.lit(to)))
        .withColumn("ValidTo", F.lead("ValidFrom", 1).over(Window.partitionBy("WWICustomerID").orderBy("ValidFrom")))
        .withColumn("LineageKey", F.lit(0))  # TODO populate lineage column
        .drop_duplicates(['WWICustomerID', 'ValidFrom'])
    )

    if to:
        customers_staging = customers_staging.where((F.col('c.LoadDatetime') <= to))

    customers_staging = (
        customers_staging
        .withColumn("WWICustomerIDGroupRowNumber", F.row_number().over(Window.partitionBy("WWICustomerID").orderBy(F.col("ValidFrom").desc())))
        .withColumn("ValidTo", (F.when((F.col("WWICustomerIDGroupRowNumber") == 1) & (F.col("RawValidTo") > F.col("DataProductLoadDatetime")), F.col("EndOfTime"))
                                .when((F.col("WWICustomerIDGroupRowNumber") == 1) & (F.col("RawValidTo") < F.col("DataProductLoadDatetime")), F.col("RawValidTo"))
                                .when((F.col("WWICustomerIDGroupRowNumber") != 1), F.col("ValidTo"))
                                ))
    )

    customers_staging = (
        customers_staging
        .drop("LoadDatetime")
        .drop("RawValidTo")
        .drop("DataProductLoadDatetime")
        .drop("WWICustomerIDGroupRowNumber")
        .drop("EndOfTime")
    )

    if offset == 0:
        default_df = (
            SparkSession
            .getActiveSession()
            .createDataFrame(
                [(0, 'Unknown', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', None, None, 0)],
                customers_staging.schema
            )
            .withColumn('ValidFrom', initial_load_date)
            .withColumn('ValidTo', end_of_time)
        )

        default_df = zip_with_row_number_as_key(default_df, key_name=primary_key_name)

        return default_df.union(zip_with_row_number_as_key(customers_staging.orderBy('ValidFrom', 'WWICustomerID'),
                                                           key_name=primary_key_name, offset=1))
    else:
        return zip_with_row_number_as_key(customers_staging, key_name=primary_key_name, offset=offset)

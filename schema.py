from pyspark.sql.types import *

# Define raw transaction schema
sales_schema = StructType([
        StructField("Store", StringType(), True),
        StructField("Cust No", StringType(), True),
        StructField("Cust Card No", StringType(), True),
        StructField("Cust Type", StringType(), True),
        StructField("Remark", StringType(), True),
        StructField("Card Online Fg", StringType(), True),
        StructField("Sale Dy", StringType(), True),
        StructField("Pos No", StringType(), True),
        StructField("Trd No", StringType(), True),
        StructField("Cust Nm", StringType(), True),
        StructField("Prod Cd", StringType(), True),
        StructField("Srcmk Cd", StringType(), True),
        StructField("Prod Nm", StringType(), True),
        StructField("Prod Type", StringType(), True),
        StructField("L1 Cd", StringType(), True),
        StructField("L1 Nm", StringType(), True),
        StructField("L2 Cd", StringType(), True),
        StructField("L2 Nm", StringType(), True),
        StructField("L3 Cd", StringType(), True),
        StructField("L3 Nm", StringType(), True),
        StructField("L4 Cd", StringType(), True),
        StructField("L4 Nm", StringType(), True),
        StructField("Buy Amt", LongType(), True),
        StructField("Sale Prc", LongType(), True),
        StructField("Sale Qty", IntegerType(), True),
        StructField("Sale Amt", LongType(), True),
        StructField("Profit Rt", DoubleType(), True),
        StructField("Profit Amt", LongType(), True),
        StructField("Vat", LongType(), True),
        StructField("Disc", LongType(), True),
        StructField("Net Sale", LongType(), True),
        StructField("Order ID", StringType(), True),
        StructField("Return", StringType(), True)
])


"""
Define schema for dim table
1. Dim Store
2. Dim Calendar
3. Dim Customer
4. Dim Product + Category
5. ...

"""

# Define schema for store
store_schema = StructType ([
    StructField("Str cd", StringType(), True),
    StructField("Str nm", StringType(), True),
    StructField("Str nm 1", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Store", StringType(), True),
    StructField("Sort", IntegerType(), True),
    StructField("Latitude", DecimalType(7,5), True),
    StructField("Longitude", DecimalType(8,5), True),
    StructField("Store on GM", StringType(), True),
    StructField("Store ID on GM", StringType(), True),
    StructField("Store on ShopeeFood", StringType(), True),
    StructField("Store on GM CVS", StringType(), True),
    StructField("Store on GMD daily screen", StringType(), True),
    StructField("B2B incharge", StringType(), True),
    StructField("Cust Type", StringType(), True)
])


# Define customer schema
customer_schema = StructType ([
    StructField("Store", StringType(), True),
    StructField("store cd", StringType(), True),
    StructField("Old_Card_No", StringType(), True),
    StructField("New_Card_No", StringType(), True),
    StructField("Store2", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("Store3", StringType(), True),
    StructField("Cust type", StringType(), True),
    StructField("remark", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("Address Code", StringType(), True),
    StructField("Old Address (Ward)", StringType(), True),
    StructField("Old Address (District)", StringType(), True),
    StructField("Old Address (City)", StringType(), True),
    StructField("New Address (Ward)", StringType(), True),
    StructField("New Address (City)", StringType(), True),
    StructField("Latitude", DecimalType(9,7), True),
    StructField("Longitude", DecimalType(9,6), True),
    StructField("Contact person", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("Main/Sub_Card", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Key/Normal Account", StringType(), True),
    StructField("Non-Active/Active", StringType(), True),
    StructField("Oc Customer Number", StringType(), True),
    StructField("Seasonal Event", StringType(), True),
    StructField("Consent Agree", StringType(), True),
    StructField("Old Store", StringType(), True),
    StructField("Add New When", StringType(), True)
])


# Define for division and product category table

division_schema = StructType ([
    StructField("Division", StringType(), True),
    StructField("Division Short", StringType(), True),
    StructField("1-Cat", StringType(), True)
])


# Define product schemas
product_schema = StructType ([
    StructField("1-Cat", StringType(), True),
    StructField("1-cat nm", StringType(), True),
    StructField("2-Cat", StringType(), True),
    StructField("2-CAT NM", StringType(), True),
    StructField("3-Cat", StringType(), True),
    StructField("3-Cat NM", StringType(), True),
    StructField("4-Cat", StringType(), True),
    StructField("4-Cat NM", StringType(), True),
    StructField("Division Short", StringType(), True),
    StructField("Prod cd", StringType(), True),
    StructField("Prod nm", StringType(), True),
    StructField("Sale cd", StringType(), True),
    StructFielđ("Ven nm", StringType(), True),
    StructField("Vendor cd", StringType(), True)
])
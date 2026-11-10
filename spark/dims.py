"""Dimension tables (silver.dim_*): small code-to-label lookups shared by both paths.

The same dictionaries feed the `payment_label` column of silver.trips and the written dim tables, so
the labels cannot drift apart. Codes follow the TLC data dictionaries.
"""
PAYMENT_TYPES = {0: "Flex Fare", 1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute",
                 5: "Unknown", 6: "Voided trip"}
RATECODES = {1: "Standard rate", 2: "JFK", 3: "Newark", 4: "Nassau or Westchester",
             5: "Negotiated fare", 6: "Group ride", 99: "Unknown"}
HVFHS_COMPANIES = {"HV0002": "Juno", "HV0003": "Uber", "HV0004": "Via", "HV0005": "Lyft"}


def dim_frames(spark, zones):
    """Return {name: DataFrame} for dim_zone, dim_payment_type, dim_ratecode, dim_hvfhs_company."""
    import pyspark.sql.functions as F

    return {
        "dim_zone": zones.select(F.col("LocationID").cast("int").alias("location_id"),
                                 F.col("Borough").alias("borough"), F.col("Zone").alias("zone"),
                                 F.col("service_zone").alias("service_zone")),
        "dim_payment_type": spark.createDataFrame(list(PAYMENT_TYPES.items()),
                                                  "payment_type int, payment_label string"),
        "dim_ratecode": spark.createDataFrame(list(RATECODES.items()), "ratecode_id int, ratecode_label string"),
        "dim_hvfhs_company": spark.createDataFrame(list(HVFHS_COMPANIES.items()),
                                                   "hvfhs_license_num string, company string"),
    }


def write_dims(spark, zones, out_dir):
    for name, df in dim_frames(spark, zones).items():
        df.coalesce(1).write.mode("overwrite").parquet(f"{out_dir}/{name}")

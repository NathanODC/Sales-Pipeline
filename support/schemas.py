from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)


sales_schema = StructType(
    [
        StructField("DATE", StringType(), False),
        StructField("CE_BRAND_FLVR", IntegerType(), False),
        StructField("BRAND_NM", StringType(), False),
        StructField("Btlr_Org_LVL_C_Desc", StringType(), False),
        StructField("CHNL_GROUP", StringType(), False),
        StructField("TRADE_CHNL_DESC", StringType(), False),
        StructField("PKG_CAT", StringType(), False),
        StructField("Pkg_Cat_Desc", StringType(), False),
        StructField("TSR_PCKG_NM", StringType(), False),
        StructField("$ Volume", FloatType(), False),
        StructField("YEAR", IntegerType(), False),
        StructField("MONTH", IntegerType(), False),
        StructField("PERIOD", IntegerType(), False),
    ]
)

chn_group_schema = StructType(
    [
        StructField("TRADE_CHNL_DESC", StringType(), False),
        StructField("TRADE_GROUP_DESC", StringType(), False),
        StructField("TRADE_TYPE_DESC", StringType(), False),
    ]
)

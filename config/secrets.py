from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

SCOPE = "wildfire_secrets"

def _dbutils() -> DBUtils:
    spark = SparkSession.getActiveSession()
    return DBUtils(spark)

def get_secret(key: str, scope: str = SCOPE) -> str:
    return _dbutils().secrets.get(scope=scope, key=key).strip()
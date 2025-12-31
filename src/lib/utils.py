import json
from pyspark.sql import types


def tabel_exists(spark, catalog, database, tableName):
    
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
    .filter(f"database = '{database}' AND tableName = '{tableName}'")
    .count())

    return count == 1

def import_schema(table):
   
    with open(f'{table}.json', 'r') as open_file:
        schema_json = json.load(open_file)
    df_schema = types.StructType.fromJson(schema_json)
    return df_schema
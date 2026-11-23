from pyspark import pipelines as dp
from src.electrocasa.schemas.gold.dim_empleados import schema_dim_empleado

catalog = spark.conf.get("bundle.catalog")
schema_silver = spark.conf.get("bundle.schema_silver")
schema_gold = spark.conf.get("bundle.schema_gold")

@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.dim_empleados",
    schema=schema_dim_empleado()
)
def gold_dim_empleados():
    return (
        spark.read.table(f"{catalog}.{schema_silver}.empleados")
        .select("id_empleado", "nombre", "dni", "salario", "cargo")
    )
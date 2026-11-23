from pyspark import pipelines as dp
from src.electrocasa.schemas.gold.dotacion import schema_dotacion_empleados

catalog = spark.conf.get("bundle.catalog")


@dp.materialized_view(
    name=f"{catalog}.gold.dotacion_empleados",
    comment="Pregunta 3: Cantidad de empleados activos por sucursal al último día de cada mes (marzo-octubre 2026).",
    schema=schema_dotacion_empleados()
)
def dotacion_empleados():
    return spark.sql(f"""
        WITH fechas_base AS (
        SELECT 
            LAST_DAY(mes_inicio) AS ultimo_dia_mes
        FROM (
            SELECT EXPLODE(
                SEQUENCE(DATE '2026-03-01', DATE '2026-10-01', INTERVAL 1 MONTH)
            ) AS mes_inicio
        )
    )
    SELECT 
        EXTRACT(YEAR FROM fb.ultimo_dia_mes) AS anho,
        EXTRACT(MONTH FROM fb.ultimo_dia_mes) AS mes,
        fb.ultimo_dia_mes,
        s.sucursal_id,
        s.nombre_sucursal,
        COUNT(DISTINCT ep.id_empleado) AS cantidad_empleados_activos
    FROM fechas_base fb
    CROSS JOIN electrocasa_dev.silver.sucursales s
    LEFT JOIN electrocasa_dev.silver.empleados ep
        ON ep.sucursal_id = s.sucursal_id
        AND ep.__START_AT < (fb.ultimo_dia_mes + INTERVAL 1 DAY)
        AND (ep.__END_AT >= (fb.ultimo_dia_mes + INTERVAL 1 DAY) OR ep.__END_AT IS NULL)
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY fb.ultimo_dia_mes, s.sucursal_id
    """)
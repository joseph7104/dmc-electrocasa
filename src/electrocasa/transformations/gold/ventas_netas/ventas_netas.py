from pyspark import pipelines as dp
from src.electrocasa.schemas.gold.ventas_netas import schema_ventas_netas

catalog = spark.conf.get("bundle.catalog")


@dp.materialized_view(
    name=f"{catalog}.gold.ventas_netas",
    comment="Pregunta 1: Ventas netas, devoluciones, cantidad de ventas y ticket promedio por sucursal y mes (marzo-octubre 2026).",
    schema=schema_ventas_netas()
)
def ventas_netas_sucursal_mes():
    return spark.sql(f"""
        WITH ventas_mes AS (
        SELECT 
            EXTRACT(YEAR FROM fecha_venta) AS anho,
            EXTRACT(MONTH FROM fecha_venta) AS mes,
            sucursal_id,
            SUM(monto_total) AS ventas,
            COUNT(DISTINCT venta_id) AS cantidad_ventas
        FROM electrocasa_dev.silver.ventas
        WHERE fecha_venta >= '2026-03-01' 
        AND fecha_venta < '2026-11-01'
        GROUP BY 1, 2, 3
    ),
    devoluciones_mes AS (
        SELECT 
            EXTRACT(YEAR FROM fecha_devolucion) AS anho,
            EXTRACT(MONTH FROM fecha_devolucion) AS mes,
            sucursal_id,
            SUM(monto_reembolso) AS devoluciones
        FROM electrocasa_dev.silver.devoluciones
        WHERE fecha_devolucion >= '2026-03-01' 
        AND fecha_devolucion < '2026-11-01'
        GROUP BY 1, 2, 3
    )
    SELECT 
        COALESCE(v.anho, d.anho) AS anho,
        COALESCE(v.mes, d.mes) AS mes,
        sc.sucursal_id,
        sc.nombre_sucursal,
        COALESCE(v.ventas, 0) AS ventas,
        COALESCE(d.devoluciones, 0) AS devoluciones,
        COALESCE(v.ventas, 0) - COALESCE(d.devoluciones, 0) AS ventas_netas,
        COALESCE(v.cantidad_ventas, 0) AS cantidad_ventas,
        ROUND(
            CASE 
                WHEN COALESCE(v.cantidad_ventas, 0) > 0 THEN v.ventas / v.cantidad_ventas 
                ELSE 0 
            END, 2
        ) AS ticket_promedio
    FROM ventas_mes v
    FULL OUTER JOIN devoluciones_mes d 
        ON v.anho = d.anho 
        AND v.mes = d.mes 
        AND v.sucursal_id = d.sucursal_id
    INNER JOIN electrocasa_dev.silver.sucursales sc 
        ON sc.sucursal_id = COALESCE(v.sucursal_id, d.sucursal_id)
    ORDER BY 1, 2, 3
    """)
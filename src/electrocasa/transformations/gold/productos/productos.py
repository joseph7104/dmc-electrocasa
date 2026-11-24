from pyspark import pipelines as dp
from src.electrocasa.schemas.gold.productos import schema_productos_cantidad, schema_productos_devolucion

catalog = spark.conf.get("bundle.catalog")


@dp.materialized_view(
    name=f"{catalog}.gold.top_productos_cantidad",
    comment="Pregunta 2A: Top 10 de productos más vendidos por unidades (marzo-octubre 2026).",
    schema=schema_productos_cantidad()
)
def top_productos_cantidad():
    return spark.sql(f"""
    WITH ventas AS(
        SELECT ct.producto_id, ct.nombre_producto, SUM(vt.cantidad) cantidad FROM {catalog}.silver.ventas vt
        INNER JOIN {catalog}.silver.catalogo ct ON vt.producto_id = ct.producto_id
        WHERE vt.fecha_venta >= '2026-03-01' AND vt.fecha_venta < '2026-11-01'
        GROUP BY 1, 2
        
    )
    SELECT producto_id, nombre_producto, cantidad FROM ventas ORDER BY cantidad DESC LIMIT 10
    """)

@dp.materialized_view(
    name=f"{catalog}.gold.top_productos_devolucion",
    comment="Pregunta 2B: Top 10 de productos con mayor tasa de devolución (marzo-octubre 2026).",
    schema=schema_productos_devolucion()
)
def top_productos_devolucion():
    return spark.sql(f"""
    WITH ventas AS(
        SELECT ct.producto_id, ct.nombre_producto, COUNT(DISTINCT venta_id) cantidad_venta  FROM {catalog}.silver.ventas vt
        INNER JOIN {catalog}.silver.catalogo ct ON vt.producto_id = ct.producto_id
        WHERE vt.fecha_venta >= '2026-03-01' AND vt.fecha_venta < '2026-11-01'
        GROUP BY 1, 2
    ),
    ventas_producto AS(
        SELECT producto_id, nombre_producto, cantidad_venta FROM ventas
    ),
    devoluciones AS(
        SELECT ct.producto_id, ct.nombre_producto, COUNT(DISTINCT devolucion_id) cantidad_dev FROM {catalog}.silver.devoluciones dv
        INNER JOIN {catalog}.silver.ventas vt ON dv.venta_id = vt.venta_id -- Se validó que cada devolución tiene su venta asociada
        INNER JOIN {catalog}.silver.catalogo ct ON dv.producto_id = ct.producto_id
        WHERE fecha_devolucion >= '2026-03-01' AND fecha_devolucion < '2026-11-01'
        GROUP BY 1, 2
    ),
    devoluciones_producto AS(
        SELECT producto_id, nombre_producto, cantidad_dev FROM devoluciones
    ),
    vista_resumen AS(
        SELECT vp.producto_id, vp.nombre_producto, vp.cantidad_venta, COALESCE(dp.cantidad_dev, 0) AS cantidad_dev,ROUND(COALESCE(dp.cantidad_dev, 0) / vp.cantidad_venta, 4) AS ratio  FROM ventas_producto vp
        LEFT JOIN devoluciones_producto dp ON vp.producto_id = dp.producto_id
        ORDER BY vp.cantidad_venta DESC
    )
    SELECT producto_id, nombre_producto, ratio FROM vista_resumen ORDER BY ratio DESC LIMIT 10
    """)

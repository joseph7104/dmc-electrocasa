from pyspark import pipelines as dp
from src.electrocasa.schemas.gold.despacho import schema_despacho

catalog = spark.conf.get("bundle.catalog")


@dp.materialized_view(
    name=f"{catalog}.gold.despacho",
    comment="Pregunta 6: Tiempo promedio de entrega en horas y porcentaje de envíos devueltos por courier sobre envíos finalizados (marzo-octubre 2026).",
    schema=schema_despacho()
)
def despacho():
    return spark.sql(f"""
        WITH eventos_limpios AS (
            SELECT 
                tracking_id,
                courier,
                estado_entrega,
                fecha_modificacion
            FROM {catalog}.silver.tracking_envios
            WHERE fecha_modificacion >= '2026-03-01' 
            AND fecha_modificacion < '2026-11-01'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY tracking_id, estado_entrega, fecha_modificacion 
                ORDER BY evento_id
            ) = 1
        ),
        resumen_envio AS (
            SELECT 
                tracking_id,
                FIRST_VALUE(courier) AS courier,
                MIN(CASE WHEN estado_entrega = 'PENDIENTE' THEN fecha_modificacion END) AS fecha_pendiente,
                MIN(CASE WHEN estado_entrega = 'ENTREGADO' THEN fecha_modificacion END) AS fecha_entregado,
                MAX(CASE WHEN estado_entrega = 'DEVUELTO' THEN 1 ELSE 0 END) AS es_devuelto,
                MAX(CASE WHEN estado_entrega IN ('ENTREGADO', 'DEVUELTO') THEN 1 ELSE 0 END) AS tiene_estado_final
            FROM eventos_limpios
            GROUP BY tracking_id
        )
        SELECT 
            courier,
            ROUND(
                AVG(
                    CASE 
                        WHEN fecha_entregado IS NOT NULL AND fecha_pendiente IS NOT NULL 
                        THEN TIMESTAMPDIFF(SECOND, fecha_pendiente, fecha_entregado) / 3600.0 
                    END
                ), 2
            ) AS tiempo_promedio_horas,
            ROUND(
                SUM(es_devuelto) * 100.0 / NULLIF(COUNT(*), 0), 2
            ) AS porcentaje_devueltos,
            COUNT(*) AS total_envios_finalizados,
            SUM(es_devuelto) AS total_envios_devueltos
        FROM resumen_envio
        WHERE tiene_estado_final = 1
        GROUP BY courier
        ORDER BY courier
    """)
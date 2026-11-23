from pyspark import pipelines as dp
from src.electrocasa.schemas.gold.resenas import schema_resenas

catalog = spark.conf.get("bundle.catalog")


@dp.materialized_view(
    name=f"{catalog}.gold.resenas",
    comment="Pregunta 5: Porcentaje de reseñas negativas (calificación 1 o 2) por categoría de producto y mes (marzo-octubre 2026).",
    schema=schema_resenas()
)
def resenas():
    return spark.sql(f"""
        WITH vista_total AS(
            SELECT
            CONCAT(
                EXTRACT(YEAR FROM rs.fecha_resena),
                '-',
                EXTRACT(MONTH FROM rs.fecha_resena),
                '-',
                ct.categoria
            ) ID,
            ct.categoria,
            EXTRACT(YEAR FROM rs.fecha_resena) anho,
            EXTRACT(MONTH FROM rs.fecha_resena) mes,
            COUNT(rs.resena_id) num_resenas_total
            FROM electrocasa_dev.silver.resenas rs
            INNER JOIN electrocasa_dev.silver.catalogo ct ON rs.producto_id = ct.producto_id
            GROUP BY 1,2,3,4
            ORDER BY 1,2,3,4
        ),
        vista_2 AS(
            SELECT
            CONCAT(
                EXTRACT(YEAR FROM rs.fecha_resena),
                '-',
                EXTRACT(MONTH FROM rs.fecha_resena),
                '-',
                ct.categoria
            ) ID,
            EXTRACT(YEAR FROM rs.fecha_resena) anho,
            EXTRACT(MONTH FROM rs.fecha_resena) mes,
            ct.categoria,
            CASE
                WHEN rs.calificacion IN (1, 2) THEN 'NEGATIVO'
                ELSE 'POSITIVO'
            END categoria_calificacion,
            COUNT(rs.resena_id) num_resenas
            FROM electrocasa_dev.silver.resenas rs
            INNER JOIN electrocasa_dev.silver.catalogo ct ON rs.producto_id = ct.producto_id
            GROUP BY 1,2,3,4,5
            ORDER BY 1,2,3,4,5
        ),
        vista_cruzada AS(
            SELECT
                v2.anho,
                v2.mes,
                v2.categoria,
                v2.categoria_calificacion,
                v2.num_resenas,
                vt.num_resenas_total,
                v2.num_resenas / vt.num_resenas_total * 100 porcentaje
            FROM vista_2 v2
            LEFT JOIN vista_total vt ON v2.ID = vt.ID
            ORDER BY 1,2,3,4
        ),
        vista_resultado AS(
            SELECT anho, mes, categoria, categoria_calificacion, porcentaje FROM vista_cruzada WHERE categoria_calificacion = 'NEGATIVO'
        )
        SELECT * FROM vista_resultado
    """)    
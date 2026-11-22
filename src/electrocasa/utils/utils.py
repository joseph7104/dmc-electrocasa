from pyspark.sql import Column
from pyspark.sql.functions import col, trim, coalesce, when, substring, lit, try_to_date
from pyspark.sql.types import DateType

def parse_fecha_registro_safe(fecha_col: str | Column) -> Column:
    c = col(fecha_col) if isinstance(fecha_col, str) else fecha_col
    col_trimmed = trim(c)
    
    return coalesce(
        when(col_trimmed.rlike(r"^\d{4}-\d{2}-\d{2}$"),
             try_to_date(col_trimmed, 'yyyy-MM-dd')),
        
        when(col_trimmed.rlike(r"^\d{2}/\d{2}/\d{4}$"),
             try_to_date(col_trimmed, 'dd/MM/yyyy')),
        
        when(col_trimmed.rlike(r"^\d{2}-\d{2}-\d{4}$"),
             try_to_date(col_trimmed, 'MM-dd-yyyy')),
        
        when(col_trimmed.rlike(r"^\d{4}/\d{2}/\d{2}"),
             try_to_date(substring(col_trimmed, 1, 10), 'yyyy/MM/dd')),
        
        when(col_trimmed.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
             try_to_date(substring(col_trimmed, 1, 10), 'yyyy-MM-dd')),
        
        when(col_trimmed.rlike(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$"),
             try_to_date(substring(col_trimmed, 1, 10), 'dd/MM/yyyy')),
        
        when(col_trimmed.rlike(r"^\d{2}/\d{2}/\d{4}$"),
             try_to_date(col_trimmed, 'MM/dd/yyyy')),
        
        lit(None).cast(DateType())
    )
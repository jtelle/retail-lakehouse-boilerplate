/*******************************************************************************
 * LAKEHOUSE QUERIES - SUPERMARKET ANALYTICS (50 STORES)
 * Configuración para DuckDB + MinIO + Capas Bronze/Gold
 *******************************************************************************/

-- 1. CONFIGURACIÓN DE ENTORNO (Ejecutar siempre al abrir DBeaver)
-- -----------------------------------------------------------------------------
INSTALL httpfs;
LOAD httpfs;

SET s3_endpoint='localhost:9000';
SET s3_access_key_id='admin';
SET s3_secret_access_key='password123';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- 2. DEFINICIÓN DE CAPAS (Vistas Virtuales)
-- -----------------------------------------------------------------------------
-- Capa Bronze: Datos crudos de todos los archivos parquet generados
CREATE OR REPLACE VIEW v_bronze_ventas AS 
SELECT * FROM read_parquet('s3://lakehouse/bronze/*.parquet');

-- Capa Gold: Reporte agregado que utiliza el Dashboard
CREATE OR REPLACE VIEW v_gold_reporte AS 
SELECT * FROM read_parquet('s3://lakehouse/gold/store_report.parquet');


-- 3. QUERIES DE AUDITORÍA Y CONTROL
-- -----------------------------------------------------------------------------
-- ¿Cuántos registros totales hemos procesado hoy?
SELECT count(*) as total_filas_bronze FROM v_bronze_ventas;

-- Verificar si hay nulos sospechosos en precios o cantidades
SELECT count(*) as errores 
FROM v_bronze_ventas 
WHERE precio_unitario <= 0 OR cantidad <= 0;


-- 4. QUERIES DE NEGOCIO (CAPA BRONZE)
-- -----------------------------------------------------------------------------
-- TOP 10 Tiendas por facturación real (calculada al vuelo)
SELECT 
    tienda_id, 
    ROUND(SUM(precio_unitario * cantidad), 2) as total_ventas,
    COUNT(DISTINCT ticket_id) as tickets_emitidos
FROM v_bronze_ventas
GROUP BY tienda_id
ORDER BY total_ventas DESC
LIMIT 10;

-- Ticket Medio por Categoría
SELECT 
    categoria, 
    ROUND(AVG(precio_unitario * cantidad), 2) as ticket_medio
FROM v_bronze_ventas
GROUP BY categoria
ORDER BY ticket_medio DESC;


-- 5. QUERIES DE RENDIMIENTO (CAPA GOLD)
-- -----------------------------------------------------------------------------
-- Comparar rendimiento: ¿Qué tiendas están por debajo de la media de ventas?
WITH media_global AS (
    SELECT AVG(ingresos_totales) as promedio FROM v_gold_reporte
)
SELECT 
    tienda_id, 
    ingresos_totales,
    ROUND(ingresos_totales - (SELECT promedio FROM media_global), 2) as diferencia_vs_media
FROM v_gold_reporte
WHERE ingresos_totales < (SELECT promedio FROM media_global)
ORDER BY ingresos_totales ASC;


-- 6. EXPLORACIÓN TEMPORAL (Si añadimos timestamps más granulares)
-- -----------------------------------------------------------------------------
-- Ventas por hora (Asumiendo que el timestamp es ISO)
SELECT 
    hour(strptime(fecha, '%Y-%m-%d %H:%M:%S')) as hora_dia,
    SUM(precio_unitario * cantidad) as ventas_totales
FROM v_bronze_ventas
GROUP BY hora_dia
ORDER BY hora_dia;



-- 7 MISCELÁNEA: QUERIES ADICIONALES PARA ANÁLISIS EXPLORATORIO
SELECT tienda_id, SUM(precio_unitario * cantidad) as ventas_fresco
FROM ventas_raw
WHERE categoria = 'Alimentación Fresca'
GROUP BY tienda_id
ORDER BY ventas_fresco DESC
LIMIT 10;

SELECT 
    categoria, 
    round(sum(precio_unitario * cantidad), 2) as total_ventas,
    count(distinct ticket_id) as num_tickets,
    round(avg(cantidad), 2) as items_por_ticket
FROM ventas_raw
GROUP BY categoria
ORDER BY total_ventas DESC;


-- 8. VALIDACIÓN DE CONSISTENCIA ENTRE CAPAS
SELECT 
    b.tienda_id,
    ROUND(SUM(b.precio_unitario * b.cantidad), 2) as calculado_desde_bronze,
    g.ingresos_totales as guardado_en_gold,
    ROUND(calculado_desde_bronze - guardado_en_gold, 2) as diferencia
FROM v_bronze_ventas b
JOIN v_gold_reporte g ON b.tienda_id = g.tienda_id
GROUP BY b.tienda_id, g.ingresos_totales
ORDER BY diferencia DESC;
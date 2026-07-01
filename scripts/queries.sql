-- ====================================================================================
-- ARCHIVO: queries.sql
-- OBJETIVO: Resolución de las 4 consultas analíticas requeridas en la prueba técnica y consultas adicionales sugeridas.
-- ====================================================================================

-- ------------------------------------------------------------------------------------
-- CONSULTA 1: Recorrido completo de un vehículo entre dos fechas.
SELECT
    vehicle_id,
    event_time,
    latitude,
    longitude,
    speed,
    distance_diff_km AS distance_from_prev_point
FROM
    telemetry_history
WHERE
    vehicle_id = 'VEH_0003' -- Reemplazar según necesidad
    AND event_time >= '2024-03-04 00:00:00'
    AND event_time <= '2024-03-05 23:59:59'
ORDER BY
    event_time ASC;

-- CONSULTA 2: Top 10 vehículos con más eventos de exceso de velocidad (>80 km/h).
SELECT
    vehicle_id,
    company_id,
    COUNT(*) AS total_overspeed_events,
    MAX(speed) AS max_speed_recorded
FROM
    telemetry_history
WHERE
    speed > 80.00
GROUP BY
    vehicle_id,
    company_id
ORDER BY
    total_overspeed_events DESC
LIMIT 10;


-- CONSULTA 3: Ranking de empresas por distancia total recorrida en el período.
SELECT
    company_id,
    ROUND(SUM(distance_diff_km)::numeric, 2) AS total_distance_km,
    COUNT(DISTINCT vehicle_id) AS active_vehicles_in_period
FROM
    telemetry_history
GROUP BY
    company_id
ORDER BY
    total_distance_km DESC;


-- CONSULTA 4: Vehículos que cambiaron de conductor más de una vez en el mismo día.
WITH daily_driver_changes AS (
    SELECT
        vehicle_id,
        DATE(event_time) AS operation_date,
        COUNT(DISTINCT driver_id) AS unique_drivers_count,
        -- ARRAY_AGG recolecta los IDs para auditoría visual sin duplicados
        ARRAY_AGG(DISTINCT driver_id) AS drivers_list
    FROM
        telemetry_history
    WHERE
        driver_id IS NOT NULL
    GROUP BY
        vehicle_id,
        DATE(event_time)
)
SELECT
    vehicle_id,
    operation_date,
    unique_drivers_count,
    drivers_list
FROM
    daily_driver_changes
WHERE
    unique_drivers_count > 1
ORDER BY
    operation_date DESC,
    unique_drivers_count DESC;

-- CONSULTA 5: Detección de "Puntos Ciegos" (Zonas de baja cobertura celular).
-- Identifica cuadrantes donde los eventos sufren demoras de transmisión > 5 minutos.
WITH delay_metrics AS (
    SELECT
        ROUND(latitude, 3) AS zone_lat,
        ROUND(longitude, 3) AS zone_lon,
        EXTRACT(EPOCH FROM (received_at - event_time))/60.0 AS delay_minutes
    FROM
        telemetry_history
    WHERE
        received_at > event_time
)
SELECT
    zone_lat,
    zone_lon,
    COUNT(*) AS total_delayed_events,
    ROUND(AVG(delay_minutes)::numeric, 2) AS average_delay_minutes
FROM
    delay_metrics
WHERE
    delay_minutes > 5.0
GROUP BY
    zone_lat,
    zone_lon
ORDER BY
    total_delayed_events DESC
LIMIT 10;

-- CONSULTA 6: Monitoreo de Alarmas Activas en Tiempo Real (Centro de Control)
SELECT
    vehicle_id,
    driver_id,
    operational_state,
    fuel_level,
    odometer_km,
    latitude,
    longitude,
    CASE
        WHEN has_low_fuel_alarm AND has_overspeed_alarm THEN 'CRÍTICO: Bajo Combustible & Exceso de Velocidad'
        WHEN has_low_fuel_alarm THEN 'ALERTA: Bajo Combustible (<15%)'
        WHEN has_overspeed_alarm THEN 'PELIGRO: Exceso de Velocidad (>80 km/h)'
        WHEN has_no_signal_alarm THEN 'ALERTA: Pérdida de Señal GPS'
        ELSE 'Operación Normal'
    END AS alarm_status_details,
    updated_at AS last_telemetry_received
FROM
    fleet_current_state
WHERE
    has_low_fuel_alarm = TRUE
    OR has_overspeed_alarm = TRUE
    OR has_no_signal_alarm = TRUE
ORDER BY
    updated_at DESC;
import os
import sys
import logging
from datetime import datetime
import polars as pl

import psycopg2
from psycopg2 import extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("TelemetryPipeline")

class TelemetryPipeline:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

        self.raw_df = None
        self.clean_df = None
        self.quarantine_df = None
        self.daily_metrics_df = None

        self.metrics = {
            "start_time": None,
            "end_time": None,
            "total_records": 0,
            "accepted_records": 0,
            "rejected_records": 0,
            "error_distribution": {}
        }

    def run(self):
        """Orquestador principal del pipeline."""
        self.metrics["start_time"] = datetime.now()
        logger.info(f"Iniciando pipeline de telemetría para: {self.file_path}")

        try:
            self.ingest()
            self.validate()
            self.transform()
            self.persist()

            self.metrics["end_time"] = datetime.now()
            self.log_summary()

        except Exception as e:
            logger.critical(f"Error crítico que interrumpió la ejecución del pipeline: {str(e)}", exc_info=True)
            sys.exit(1)

    def ingest(self):
        """Lectura del archivo CSV fuente."""
        logger.info("Etapa 1: Iniciando Ingesta de datos...")
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"El archivo especificado no existe: {self.file_path}")

        self.raw_df = pl.read_csv(
            self.file_path,
            try_parse_dates=True
        )
        self.metrics["total_records"] = self.raw_df.height
        logger.info(f"Ingesta completada. Registros leídos: {self.metrics['total_records']}")

    def validate(self):
        """Aplicación de reglas de calidad y detección de registros inválidos."""
        logger.info("Etapa 2: Iniciando Validación y Control de Calidad...")
        rejected_chunks = []

        # --- Regla A: Datos Faltantes ---
        required_cols = ["vehicle_id", "event_time", "latitude", "longitude"]
        null_condition = pl.any_horizontal(pl.col(required_cols).is_null())
        df_nulls = self.raw_df.filter(null_condition).with_columns(pl.lit("Missing required fields").alias("error_reason"))
        if df_nulls.height > 0: rejected_chunks.append(df_nulls)

        valid_so_far = self.raw_df.filter(~null_condition)

        # --- Regla B: Coordenadas Inválidas ---
        invalid_coords = (
            (pl.col("latitude") < -90) | (pl.col("latitude") > 90) |
            (pl.col("longitude") < -180) | (pl.col("longitude") > 180)
        )
        df_coords = valid_so_far.filter(invalid_coords).with_columns(pl.lit("Invalid GPS coordinates").alias("error_reason"))
        if df_coords.height > 0: rejected_chunks.append(df_coords)

        # --- Regla C: Velocidades Imposibles (> 150 km/h) ---
        invalid_speed = pl.col("speed") > 150
        df_speed = valid_so_far.filter(~invalid_coords & invalid_speed).with_columns(pl.lit("Impossible speed (>150 km/h)").alias("error_reason"))
        if df_speed.height > 0: rejected_chunks.append(df_speed)

        # --- Regla D: Duplicados (Basado en tu EDA) ---
        dup_subset = ['tracking_id', 'vehicle_id', 'event_time', 'event_type']
        df_duplicates = valid_so_far.filter(
            pl.struct(dup_subset).is_duplicated() & pl.struct(dup_subset).is_first_distinct().not_()
        ).with_columns(pl.lit("Duplicate event").alias("error_reason"))
        if df_duplicates.height > 0: rejected_chunks.append(df_duplicates)

        # --- Regla E: Viajeros en el tiempo (Basado en tu EDA) ---
        invalid_time = pl.col("event_time") > pl.col("received_at")
        df_time = valid_so_far.filter(invalid_time).with_columns(pl.lit("Event time > received_at").alias("error_reason"))
        if df_time.height > 0: rejected_chunks.append(df_time)

        # Construcción de la Cuarentena
        if rejected_chunks:
            self.quarantine_df = pl.concat(rejected_chunks).unique(subset=["tracking_id"])
            self.metrics["rejected_records"] = self.quarantine_df.height

            error_counts = self.quarantine_df["error_reason"].value_counts()
            self.metrics["error_distribution"] = {row["error_reason"]: row["count"] for row in error_counts.iter_rows(named=True)}
        else:
            self.quarantine_df = pl.DataFrame(schema={**self.raw_df.schema, "error_reason": pl.Utf8})
            self.metrics["rejected_records"] = 0

        # DataFrame Limpio
        if self.metrics["rejected_records"] > 0:
            self.clean_df = self.raw_df.filter(~pl.col("tracking_id").is_in(self.quarantine_df["tracking_id"]))
        else:
            self.clean_df = self.raw_df

        self.metrics["accepted_records"] = self.clean_df.height
        logger.info(f"Validación completada. Aceptados: {self.metrics['accepted_records']} | Rechazados: {self.metrics['rejected_records']}")

    def transform(self):
        """Normalización, enriquecimiento y cálculo de campos derivados."""
        logger.info("Etapa 3: Iniciando Transformación y Enriquecimiento...")
        import math

        if self.clean_df.height == 0:
            return

        # Orden cronológico por vehículo
        self.clean_df = self.clean_df.sort(["vehicle_id", "event_time"])

        # Generar columnas desplazadas (LAG)
        self.clean_df = self.clean_df.with_columns([
            pl.col("latitude").shift(1).over("vehicle_id").alias("prev_latitude"),
            pl.col("longitude").shift(1).over("vehicle_id").alias("prev_longitude"),
            pl.col("event_time").shift(1).over("vehicle_id").alias("prev_event_time"),
            pl.col("odometer_km").shift(1).over("vehicle_id").alias("prev_odometer")
        ])

        # Detectar Reinicio de Odómetro
        self.clean_df = self.clean_df.with_columns(
            (pl.col("odometer_km") < pl.col("prev_odometer")).fill_null(False).alias("is_odometer_reset")
        )

        # Diferencia de Tiempo en Segundos
        self.clean_df = self.clean_df.with_columns(
            (pl.col("event_time") - pl.col("prev_event_time")).dt.total_seconds().fill_null(0).alias("time_diff_seconds")
        )

        # Fórmula Haversine vectorizada para calcular distancias en superficies esfericas
        # Convertir a radianes
        lat1 = pl.col("prev_latitude") * (math.pi / 180.0)
        lat2 = pl.col("latitude") * (math.pi / 180.0)
        lon1 = pl.col("prev_longitude") * (math.pi / 180.0)
        lon2 = pl.col("longitude") * (math.pi / 180.0)
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = (dlat / 2).sin()**2 + lat1.cos() * lat2.cos() * (dlon / 2).sin()**2
        c = 2 * a.sqrt().arcsin()
        R = 6371.0
        self.clean_df = self.clean_df.with_columns(
            (R * c).fill_null(0.0).alias("distance_diff_km")
        )
        # Generar las Métricas Operativas Diarias
        logger.info("Calculando métricas operativas diarias...")
        self.daily_metrics_df = (
            self.clean_df
            .with_columns(
                pl.col("event_time").dt.date().alias("operational_date") # <-- Corregido
            )
            .group_by(["operational_date", "vehicle_id", "company_id"])
            .agg([
                pl.col("distance_diff_km").sum().round(2).alias("total_distance_km"),
                pl.col("speed").mean().round(2).alias("avg_speed_kmh"), # <-- Corregido
                pl.col("speed").max().round(2).alias("max_speed_kmh"), # <-- Corregido
                (pl.col("fuel_level").first() - pl.col("fuel_level").last()).round(2).alias("estimated_fuel_consumption_pct"), # <-- Corregido
                (pl.col("speed") > 80).sum().alias("overspeed_events_count")
            ])
        )
        # Limpiar columnas temporales de cálculo
        self.clean_df = self.clean_df.drop(["prev_latitude", "prev_longitude", "prev_event_time", "prev_odometer"])
        logger.info("Transformación completada con éxito.")

    def persist(self):
        """Almacenamiento de los datos procesados en la base de datos destino."""
        logger.info("Etapa 4: Iniciando Persistencia en PostgreSQL...")
        conn_str = f"host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')}"

        try:
            with psycopg2.connect(conn_str) as conn:
                with conn.cursor() as cursor:
                    if self.quarantine_df.height > 0:
                        logger.info(f"Insertando {self.quarantine_df.height} registros en 'quarantine_events'...")
                        quarantine_cols = self.quarantine_df.columns
                        quarantine_data = self.quarantine_df.rows()
                        query_quarantine = f"""
                            INSERT INTO quarantine_events ({', '.join(quarantine_cols)})
                            VALUES %s
                            ON CONFLICT (tracking_id) DO NOTHING
                        """
                        extras.execute_values(cursor, query_quarantine, quarantine_data, page_size=10000)

                    if self.clean_df.height > 0:

                        logger.info(f"Persistiendo {self.clean_df.height} registros en 'telemetry_history'...")

                        history_cols = [
                            "tracking_id", "vehicle_id", "company_id", "route_id", "driver_id",
                            "event_time", "received_at", "latitude", "longitude", "speed",
                            "fuel_level", "battery_level", "odometer_km", "passenger_count",
                            "event_type", "distance_diff_km", "time_diff_seconds", "is_odometer_reset"
                        ]

                        history_data = self.clean_df.select(history_cols).rows()

                        query_history = f"""
                            INSERT INTO telemetry_history ({', '.join(history_cols)})
                            VALUES %s
                            ON CONFLICT (tracking_id) DO NOTHING
                        """
                        extras.execute_values(cursor, query_history, history_data, page_size=10000)

                        logger.info("Actualizando tabla de 'fleet_current_state'...")
                        # Obtener el último evento de cada vehículo
                        fleet_df = self.clean_df.sort(["vehicle_id", "event_time"]).unique(subset=["vehicle_id"], keep="last")

                        # Derivar las alarmas y el estado operativo
                        fleet_df = fleet_df.with_columns([
                            pl.when(pl.col("speed") > 0).then(pl.lit("en ruta")).otherwise(pl.lit("detenido")).alias("operational_state"),
                            (pl.col("fuel_level") < 15).fill_null(False).alias("has_low_fuel_alarm"),
                            (pl.col("speed") > 80).fill_null(False).alias("has_overspeed_alarm")
                        ])
                        fleet_cols = [
                            "vehicle_id", "tracking_id", "event_time", "latitude", "longitude",
                            "operational_state", "driver_id", "fuel_level", "odometer_km",
                            "has_low_fuel_alarm", "has_overspeed_alarm"
                        ]
                        fleet_data = fleet_df.select(fleet_cols).rows()
                        # UPSERT: Si el vehículo ya existe, actualiza sus datos con la nueva posición
                        query_fleet = f"""
                            INSERT INTO fleet_current_state ({', '.join(fleet_cols)})
                            VALUES %s
                            ON CONFLICT (vehicle_id) DO UPDATE SET
                                tracking_id = EXCLUDED.tracking_id,
                                event_time = EXCLUDED.event_time,
                                latitude = EXCLUDED.latitude,
                                longitude = EXCLUDED.longitude,
                                operational_state = EXCLUDED.operational_state,
                                driver_id = EXCLUDED.driver_id,
                                fuel_level = EXCLUDED.fuel_level,
                                odometer_km = EXCLUDED.odometer_km,
                                has_low_fuel_alarm = EXCLUDED.has_low_fuel_alarm,
                                has_overspeed_alarm = EXCLUDED.has_overspeed_alarm,
                                updated_at = CURRENT_TIMESTAMP
                        """
                        extras.execute_values(cursor, query_fleet, fleet_data, page_size=10000)

                        if self.daily_metrics_df is not None and self.daily_metrics_df.height > 0:
                            logger.info(f"Persistiendo {self.daily_metrics_df.height} registros en 'daily_operational_metrics'...")
                            daily_cols = [
                                "vehicle_id", "company_id", "operational_date", "total_distance_km", 
                                "avg_speed_kmh", "max_speed_kmh", "estimated_fuel_consumption_pct", "overspeed_events_count"
                            ]

                            daily_data = self.daily_metrics_df.select(daily_cols).rows()

                            query_daily = f"""
                                INSERT INTO daily_operational_metrics ({', '.join(daily_cols)})
                                VALUES %s
                                ON CONFLICT (vehicle_id, operational_date) DO UPDATE SET
                                    total_distance_km = EXCLUDED.total_distance_km,
                                    avg_speed_kmh = EXCLUDED.avg_speed_kmh,
                                    max_speed_kmh = EXCLUDED.max_speed_kmh,
                                    estimated_fuel_consumption_pct = EXCLUDED.estimated_fuel_consumption_pct,
                                    overspeed_events_count = EXCLUDED.overspeed_events_count,
                                    updated_at = CURRENT_TIMESTAMP
                            """
                            extras.execute_values(cursor, query_daily, daily_data, page_size=10000)

                    cursor.execute("SELECT COUNT(*) FROM telemetry_history")
                    total_db_history = cursor.fetchone()[0]
                    cursor.execute("SELECT COUNT(*) FROM quarantine_events")
                    total_db_quarantine = cursor.fetchone()[0]

                    logger.info("==================================================")
                    logger.info(" AUDITORÍA DE IDEMPOTENCIA EN BASE DE DATOS")
                    logger.info(f" -> Total histórico en DB:  {total_db_history} filas")
                    logger.info(f" -> Total cuarentena en DB: {total_db_quarantine} filas")
                    logger.info("==================================================")

                conn.commit()
                logger.info("Persistencia finalizada correctamente.")
        except Exception as e:
            logger.error(f"Error durante la persistencia en base de datos: {str(e)}")
            raise

    def log_summary(self):
        """Imprime métricas de auditoría requeridas al finalizar la ejecución."""
        duration = self.metrics["end_time"] - self.metrics["start_time"]

        logger.info("==================================================")
        logger.info("         RESUMEN DE EJECUCIÓN DEL PIPELINE        ")
        logger.info("==================================================")
        logger.info(f"Tiempo de Inicio:     {self.metrics['start_time']}")
        logger.info(f"Tiempo de Fin:        {self.metrics['end_time']}")
        logger.info(f"Duración Total:       {duration.total_seconds():.2f} segundos")
        logger.info(f"Total Registros:      {self.metrics['total_records']}")
        logger.info(f"Registros Aceptados:  {self.metrics['accepted_records']}")
        logger.info(f"Registros Rechazados: {self.metrics['rejected_records']}")
        logger.info("--------------------------------------------------")
        logger.info("Distribución de Errores en Cuarentena:")
        for err, count in self.metrics["error_distribution"].items():
            logger.info(f" - {err}: {count}")
        logger.info("==================================================")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Debe proporcionar la ruta del archivo CSV como argumento.")
        sys.exit(1)

    csv_param = sys.argv[1]
    pipeline = TelemetryPipeline(csv_param)
    pipeline.run()
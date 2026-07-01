# Pipeline de Procesamiento de Telemetría (Data Engineering)

Este repositorio contiene la solución a la prueba técnica para el rol. El proyecto implementa un pipeline ETL/ELT dockerizado que procesa más de 250,000 eventos diarios de telemetría GPS, aplicando validaciones de calidad de datos, cálculos geoespaciales y persistencia idempotente en PostgreSQL.

## Stack Tecnológico y Decisiones de Herramientas

* **Lenguaje:** Python 3.11+
* **Procesamiento de Datos:** **Polars**. Aunque Pandas es el estándar de la industria (y fue sugerido en la prueba), decidí implementar la solución usando Polars tras investigar sobre su rendimiento. Al procesar un volumen considerable de datos (+250k filas), el motor basado en Rust de Polars, su evaluación perezosa (lazy evaluation) y su paralelismo natural me permitieron realizar transformaciones mucho más rápidas y eficientes en memoria.
* **Base de Datos:** PostgreSQL
* **Infraestructura:** Docker & Docker Compose

---

## Instrucciones de Ejecución

El proyecto está completamente contenerizado, por lo que no necesita instalar dependencias locales más allá de Docker.

### 0. Preparación del archivo de datos (Requisito Previo)
Por buenas prácticas de control de versiones y dado el tamaño del archivo generado para esta prueba, los datos crudos no están incluidos en el repositorio.

Antes de ejecutar los contenedores, asegúrate de pegar el archivo provisto por la empresa en la **raíz de este proyecto** y verificar que se llame **exactamente**:
`telemetria.csv`
*(Nota: Este nombre exacto es vital para que Docker Compose pueda mapear el volumen correctamente hacia el contenedor del pipeline).*

### 1. Levantar el pipeline por primera vez

Para construir las imágenes, levantar la base de datos PostgreSQL, ejecutar las migraciones (`schema.sql`) y correr el script de Python automáticamente, ejecuta en la terminal:

```bash
docker compose up --build

```

### 2. Reiniciar el entorno (Borrar persistencia)

Si deseas hacer pruebas desde cero y necesitas borrar los datos de la base de datos (destruyendo el volumen de PostgreSQL), ejecuta:

```bash
docker compose down -v

```

---

## Supuestos y Decisiones Arquitectónicas

Durante el desarrollo del Análisis Exploratorio de Datos (EDA) y la construcción del pipeline, tomé varias decisiones de diseño enfocadas en mantener la integridad de la operación del negocio:

### 1. Cálculo de Distancias: La Fórmula Haversine

Dado que las coordenadas GPS (latitud y longitud) representan puntos sobre una superficie esférica (la Tierra), usar distancias euclidianas (líneas rectas planas) genera un margen de error inaceptable para medir el kilometraje de una flota. Para solucionar esto, investigué e implementé la **Fórmula Haversine** vectorizada nativamente en Polars. Esto permite calcular la distancia ortodrómica real y precisa entre dos puntos sin sacrificar el rendimiento del pipeline.

### 2. Umbral de Eventos Retrasados

Para los análisis de latencia en la transmisión de datos, decidí establecer arbitrariamente un umbral de **5 minutos** de diferencia entre `event_time` y `received_at` para clasificar un evento como "retrasado" o "tardío", al no haber un umbral específico detallado en los requerimientos.

### 3. Cuarentena vs. Realidad Operativa (Falsos Positivos)

Durante el análisis, detecté dos patrones que inicialmente podrían parecer anomalías, pero decidí **NO** enviarlos a la tabla de cuarentena (`quarantine_events`), sino mantenerlos en el flujo principal y analizarlos vía SQL:

* **Eventos Tardíos (Zonas de baja cobertura):** Los registros con gran desfase entre la ocurrencia del evento y su llegada al servidor no son datos corruptos. Corresponden a una realidad de IoT conocida como "Store and Forward": cuando un vehículo entra a un túnel, sótano o zona rural sin señal, guarda los datos y los transmite de golpe al recuperar conexión. Si enviara estos datos a cuarentena, arruinaría el cálculo de la distancia (Haversine), ya que parecería que el vehículo se teletransportó. Por ello, lo dejé como un caso de uso analítico para detectar "Puntos Ciegos" geográficos (Ver Consulta 5 en `queries.sql`).
* **Múltiples cambios de conductor al día:** Aunque un mismo vehículo cambiando de `driver_id` repetidamente puede parecer anómalo, en la realidad operativa del transporte masivo, un conductor puede enfermarse, tener una calamidad o simplemente cambiar de turno. Mandar esto a cuarentena ocultaría información vital de RRHH. Decidí que este escenario es valioso y debe monitorearse como una métrica analítica (Ver Consulta 4 en `queries.sql`).

### 4. Estrategia de Idempotencia Dura
Para garantizar que la ejecución múltiple del pipeline sobre el mismo archivo no duplique registros ni corrompa las métricas, implementé una estrategia de **Idempotencia Dura** a nivel de motor de base de datos (PostgreSQL), utilizando cláusulas nativas `ON CONFLICT`:

* **Histórico y Cuarentena (`DO NOTHING`):** Se establecieron llaves primarias y restricciones `UNIQUE` (ej. sobre `tracking_id`). Si el registro ya existe, la base de datos lo ignora silenciosamente a una velocidad óptima.
* **Métricas y Estado Actual (`DO UPDATE`):** Para tablas de estado consolidado (`fleet_current_state`, `daily_operational_metrics`), se configuró un "Upsert". Si el pipeline se repite, los valores se actualizan sin generar filas duplicadas.

> **Prueba de Auditoría:** El archivo `docker-compose.yml` está configurado para ejecutar el pipeline **dos veces seguidas** intencionalmente. Al revisar los logs de la terminal, se comprueba cómo la base de datos bloquea los duplicados en la segunda ejecución, manteniendo intacto el conteo final de registros.

---

## Consultas Analíticas (SQL)

Las respuestas a las consultas solicitadas por el negocio se encuentran en el archivo `queries.sql`. Éstas consultan directamente las tablas físicas optimizadas e indexadas que el pipeline genera (`telemetry_history`, `daily_operational_metrics`, y `fleet_current_state`).

---

## Propuesta de Inteligencia Operativa (AI / ML)

Con base en el Análisis Exploratorio de Datos (EDA) y la telemetría disponible, propongo la siguiente arquitectura y casos de uso de Machine Learning para pasar de un enfoque reactivo a uno predictivo, maximizando la eficiencia de la flota:

### Caso de Uso 1: Mapeo Predictivo de "Puntos Ciegos" (Zonas sin Cobertura)
* **El Problema:** El patrón "Store and Forward" genera eventos tardíos. Actualmente, si el Centro de Control deja de recibir señal, no sabe si el vehículo fue robado, se averió, o simplemente entró a un túnel.
* **Técnica ML:** Algoritmo de Clustering Espacial (**DBSCAN**) aplicado sobre las coordenadas (`latitude`, `longitude`) de los eventos con alta latencia (`received_at - event_time > 5 min`).
* **Impacto Operativo:** El modelo delimitará dinámicamente "geocercas de sombra". El sistema de monitoreo cruzará la posición actual del vehículo con estas geocercas, alertando al despachador *antes* de que el bus pierda la señal de forma predecible, mitigando falsas alarmas de pánico.

### Caso de Uso 2: Detección de Extracción No Autorizada de Combustible
* **El Problema:** Las caídas de combustible por robo ("ordeño") son difíciles de distinguir del consumo normal sin una revisión manual intensiva.
* **Técnica ML:** Modelo de detección de anomalías en series de tiempo (ej. **Isolation Forest** o **Prophet**) usando las variables `fuel_level`, `speed`, y la distancia calculada (`distance_diff_km`).
* **Impacto Operativo:** El modelo aprenderá la curva normal de degradación de combustible. Si detecta una caída súbita del porcentaje de `fuel_level` mientras el vehículo está detenido (`speed = 0`), disparará una alerta crítica en tiempo real al equipo de seguridad.

### Viabilidad y Arquitectura Propuesta (MLOps)
Dado el stack actual, la implementación es altamente viable:
1. **Feature Store / Entrenamiento:** Las tablas `telemetry_history` y `daily_operational_metrics` servirán como fuente para entrenar los modelos *offline* (usando Apache Airflow para orquestar reentrenamientos semanales).
2. **Inferencia en Tiempo Real:** El modelo entrenado puede empaquetarse con MLflow y desplegarse como una API (FastAPI) en un contenedor Docker.
3. **Flujo Streaming:** Antes de persistir los datos en PostgreSQL, los eventos que lleguen por Kafka/MQTT pasarán por esta API para inferir anomalías al vuelo, enriqueciendo la tabla `fleet_current_state` con un "Score de Riesgo".
#!/usr/bin/with-contenv bashio

SERIAL_PORT=$(bashio::config 'serial_port')
BAUDRATE=$(bashio::config 'baudrate')
UNIT_ID=$(bashio::config 'unit_id')
MQTT_BASE_TOPIC_CFG=$(bashio::config 'mqtt_base_topic')

# De 'mqtt:want' service-binding in config.yaml zorgt dat de Supervisor
# hier automatisch de connectiegegevens van de Mosquitto-add-on aanreikt -
# geen hardcoded host/poort/wachtwoord nodig.
export MQTT_HOST=$(bashio::services mqtt "host")
export MQTT_PORT=$(bashio::services mqtt "port")
export MQTT_USER=$(bashio::services mqtt "username")
export MQTT_PASSWORD=$(bashio::services mqtt "password")
export MQTT_BASE_TOPIC="${MQTT_BASE_TOPIC_CFG}"

bashio::log.info "Start laadpaal RTU-slave op ${SERIAL_PORT} (baud=${BAUDRATE}, unit_id=${UNIT_ID}, mqtt=${MQTT_HOST}:${MQTT_PORT})"

exec python3 /laadpaal_rtu_slave.py --port "${SERIAL_PORT}" --baudrate "${BAUDRATE}" --unit-id "${UNIT_ID}"

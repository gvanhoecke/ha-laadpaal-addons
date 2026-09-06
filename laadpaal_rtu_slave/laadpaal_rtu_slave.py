#!/usr/bin/env python3
"""
laadpaal_rtu_slave.py
----------------------
Modbus RTU slave (server) die rechtstreeks op de RS485-bus van de laadpaal
draait, en zijn registerwaarden bijgewerkt krijgt via MQTT vanuit je
bestaande Node-RED flow. Vervangt de RTU->TCP gateway.

Register-map hieronder is overgenomen uit de N380 CT user manual (Inepro
Metering, V1.14, hoofdstuk 8 "Modbus register file"). Adressen in dat
document zijn hexadecimaal (bv. "400B", "501C") en worden hier naar
decimaal omgezet voor gebruik in pymodbus.

Werking:
  Node-RED (berekent gewenste "meter"-waarden)
      -> publiceert op MQTT-topics
      -> dit script (MQTT-subscriber, draait in aparte thread)
      -> schrijft waarden in de Modbus-datastore
      -> pymodbus RTU-server antwoordt de laadpaal rechtstreeks over RS485

Vereisten (op de Pi, binnen een virtualenv of met --break-system-packages):
    pip install pymodbus paho-mqtt --break-system-packages

LET OP - onduidelijkheden in de bron-documentatie:
  - Register 501C komt in de manual TWEE keer voor: eenmaal als "L1
    Reactive power" en eenmaal als "L3 Reactive power". Dat is
    vrijwel zeker een fout/typo in de PDF (waarschijnlijk moet L3
    Reactive power op 5020 staan, gezien het patroon van +2 per
    fase-register). Hieronder gebruik ik 5020 voor L3 Reactive power
    op basis van dat patroon - controleer dit met een Modbus-scan
    tool tegen je fysieke meter voor je hierop vertrouwt.
  - "Length 2 signed" bij identificatie-registers interpreteer ik als
    32-bit signed integer over 2 registers; "1 signed" als 16-bit
    signed integer. Dit staat niet expliciet zo in de manual maar volgt
    uit de context (functiecode 03, lengte in registers).
"""

import argparse
import logging
import os
import signal
import struct
import sys
import threading
import time
from dataclasses import dataclass
from typing import Literal

import paho.mqtt.client as mqtt
from pymodbus.datastore import (
    ModbusSparseDataBlock,
    ModbusServerContext,
    ModbusSlaveContext,
)
from pymodbus.server import StartSerialServer

# --------------------------------------------------------------------------
# CONFIGURATIE - pas dit aan naar jouw situatie
# --------------------------------------------------------------------------

SERIAL_PORT = "/dev/ttyUSB0"     # RS485-adapter poort - check met `ls /dev/serial/by-id/`
BAUDRATE = 9600                  # N380 CT default (zie manual hoofdstuk 6)
PARITY = "E"                     # N380 CT default = Even (zie manual hoofdstuk 6)
STOPBITS = 1
BYTESIZE = 8
SLAVE_UNIT_ID = 1                # N380 CT default Modbus ID = 01

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")   # HA add-on 'mqtt:want' service zet dit automatisch
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USERNAME = os.environ.get("MQTT_USER") or None
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD") or None
MQTT_BASE_TOPIC = os.environ.get("MQTT_BASE_TOPIC", "laadpaal/meter")

STALE_WARNING_SECONDS = 15       # waarschuw als een MQTT-key te lang niet ververst
SNAPSHOT_LOG_INTERVAL_SECONDS = 300  # periodiek overzicht i.p.v. per-bericht logging

# --- Heartbeat / Last Will (voor monitoring vanuit Home Assistant) ---
STATUS_TOPIC = f"{MQTT_BASE_TOPIC}/status"   # "online" (heartbeat, retained) / "offline" (LWT, retained)
HEARTBEAT_INTERVAL_SECONDS = 5


DType = Literal["float32", "int32", "int16"]


@dataclass
class RegisterDef:
    """Definieert waar en hoe een waarde in de Modbus-registers komt."""
    address: int                 # decimaal register-adres (raw, niet Modicon-offset)
    dtype: DType = "float32"     # float32 = 2 regs, int32 = 2 regs, int16 = 1 reg
    scale: float = 1.0           # vermenigvuldigingsfactor, indien nodig
    word_order_big_endian: bool = True  # manual specificeert "Float - (ABCD)" = big-endian


def hexreg(h: str) -> int:
    """Hulpfunctie: hex-adres uit de manual (bv. '501C') naar decimaal."""
    return int(h, 16)


# --------------------------------------------------------------------------
# REGISTER MAP - overgenomen uit N380 CT user manual, hoofdstuk 8
# --------------------------------------------------------------------------
# Topic wordt gepubliceerd als: laadpaal/meter/<key>
# Payload: platte numerieke waarde (bv. "2350.5"), geen JSON nodig.

REGISTER_MAP: dict[str, RegisterDef] = {
    # --- Spanning (V) ---
    "voltage":              RegisterDef(hexreg("5000"), "float32"),
    "voltage_l1":            RegisterDef(hexreg("5002"), "float32"),
    "voltage_l2":            RegisterDef(hexreg("5004"), "float32"),
    "voltage_l3":            RegisterDef(hexreg("5006"), "float32"),

    # --- Frequentie (Hz) ---
    "grid_frequency":        RegisterDef(hexreg("5008"), "float32"),

    # --- Stroom (A) ---
    "current":               RegisterDef(hexreg("500A"), "float32"),
    "current_l1":            RegisterDef(hexreg("500C"), "float32"),
    "current_l2":            RegisterDef(hexreg("500E"), "float32"),
    "current_l3":            RegisterDef(hexreg("5010"), "float32"),

    # --- Actief vermogen (kW) ---
    "active_power_total":    RegisterDef(hexreg("5012"), "float32"),
    "active_power_l1":       RegisterDef(hexreg("5014"), "float32"),
    "active_power_l2":       RegisterDef(hexreg("5016"), "float32"),
    "active_power_l3":       RegisterDef(hexreg("5018"), "float32"),

    # --- Reactief vermogen (kVA) ---
    "reactive_power_total":  RegisterDef(hexreg("501A"), "float32"),
    "reactive_power_l1":     RegisterDef(hexreg("501C"), "float32"),
    "reactive_power_l2":     RegisterDef(hexreg("501E"), "float32"),
    # LET OP: adres van L3 reactive power is onduidelijk in de manual
    # (zelfde "501C" als L1 vermeld - vermoedelijk typo). Hier op basis
    # van het +2-patroon ingevuld als 5020; verifieer dit fysiek.
    "reactive_power_l3":     RegisterDef(hexreg("5020"), "float32"),

    # --- Schijnbaar vermogen (kVA) ---
    "apparent_power_total":  RegisterDef(hexreg("5022"), "float32"),
    "apparent_power_l1":     RegisterDef(hexreg("5024"), "float32"),
    "apparent_power_l2":     RegisterDef(hexreg("5026"), "float32"),
    "apparent_power_l3":     RegisterDef(hexreg("5028"), "float32"),

    # --- Vermogensfactor ---
    "power_factor_total":    RegisterDef(hexreg("502A"), "float32"),
    "power_factor_l1":       RegisterDef(hexreg("502C"), "float32"),
    "power_factor_l2":       RegisterDef(hexreg("502E"), "float32"),
    "power_factor_l3":       RegisterDef(hexreg("5030"), "float32"),

    # --- Energie (kWh) ---
    "active_energy_total":   RegisterDef(hexreg("6000"), "float32"),
    "active_energy_l1":      RegisterDef(hexreg("6006"), "float32"),
    "active_energy_forward": RegisterDef(hexreg("600C"), "float32"),
    "active_energy_reverse": RegisterDef(hexreg("6018"), "float32"),
}

# --------------------------------------------------------------------------
# STATISCHE / IDENTIFICATIE-REGISTERS - eenmalig bij opstart ingevuld,
# NIET via MQTT bijgewerkt. Sommige laadpalen lezen deze uit tijdens
# initialisatie/handshake (serienummer, versie, meter-ID, ...).
# Pas de waarden aan naar wat jouw laadpaal verwacht/accepteert.
# --------------------------------------------------------------------------

STATIC_REGISTERS: dict[str, tuple[RegisterDef, float]] = {
    "serial_number":       (RegisterDef(hexreg("4000"), "int32"), 12345678),
    "meter_code":          (RegisterDef(hexreg("4002"), "int16"), 1),
    "meter_id":            (RegisterDef(hexreg("4003"), "int16"), SLAVE_UNIT_ID),
    "baud_rate":           (RegisterDef(hexreg("4004"), "int16"), BAUDRATE),
    "protocol_version":    (RegisterDef(hexreg("4005"), "float32"), 1.0),
    "software_version":    (RegisterDef(hexreg("4007"), "float32"), 1.14),
    "hardware_version":    (RegisterDef(hexreg("4009"), "float32"), 1.0),
    "meter_amps":          (RegisterDef(hexreg("400B"), "int32"), 80),   # CT primary current
    "parity_setting":      (RegisterDef(hexreg("4011"), "int16"), 1),   # 1=Even (manual default)
    "software_version_crc":(RegisterDef(hexreg("401B"), "int32"), 0),
    "combination_code":    (RegisterDef(hexreg("400F"), "int16"), 5),   # C05 = forward+reverse (default)
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)
log = logging.getLogger("laadpaal_rtu_slave")

# pymodbus logt standaard een regel per Modbus-transactie (elke poll van
# de laadpaal, dus mogelijk elke 1-2 seconden). Dat loopt op tot
# tienduizenden regels per dag - expliciet dempen, los van het
# root-logniveau hierboven.
logging.getLogger("pymodbus").setLevel(logging.WARNING)
logging.getLogger("pymodbus.server").setLevel(logging.WARNING)
logging.getLogger("pymodbus.logging").setLevel(logging.WARNING)
# paho-mqtt kan ook per bericht loggen als je zijn logger aan basicConfig hangt
logging.getLogger("paho").setLevel(logging.WARNING)

# --------------------------------------------------------------------------
# Encodering
# --------------------------------------------------------------------------

def value_to_registers(value: float, reg_def: RegisterDef) -> list[int]:
    scaled = value * reg_def.scale
    if reg_def.dtype == "float32":
        packed = struct.pack(">f", scaled)
        hi, lo = struct.unpack(">HH", packed)
    elif reg_def.dtype == "int32":
        packed = struct.pack(">i", int(round(scaled)))
        hi, lo = struct.unpack(">HH", packed)
    elif reg_def.dtype == "int16":
        return [int(round(scaled)) & 0xFFFF]
    else:
        raise ValueError(f"Onbekend dtype: {reg_def.dtype}")
    return [hi, lo] if reg_def.word_order_big_endian else [lo, hi]


# --------------------------------------------------------------------------
# Modbus datastore opzetten (sparse, want adresruimte loopt van 0x4000
# tot 0x6018 met grote gaten ertussen - ModbusSparseDataBlock is hiervoor
# geschikter/zuiniger dan een aaneengesloten block)
# --------------------------------------------------------------------------

initial_values: dict[int, int] = {}
for _key, (_rd, _val) in STATIC_REGISTERS.items():
    _regs = value_to_registers(_val, _rd)
    for _i, _r in enumerate(_regs):
        initial_values[_rd.address + _i] = _r
for _key, _rd in REGISTER_MAP.items():
    for _i in range(2 if _rd.dtype != "int16" else 1):
        initial_values.setdefault(_rd.address + _i, 0)

holding_block = ModbusSparseDataBlock(initial_values)
slave_context = ModbusSlaveContext(
    di=ModbusSparseDataBlock({0: 0}),
    co=ModbusSparseDataBlock({0: 0}),
    hr=holding_block,
    ir=ModbusSparseDataBlock({0: 0}),
    zero_mode=True,
)


def build_server_context(unit_id: int) -> ModbusServerContext:
    """Bouwt de server context met het opgegeven unit-ID. Eerder werd dit
    altijd met de module-constante SLAVE_UNIT_ID gebouwd, ook als
    --unit-id iets anders meegaf - dat is hiermee gecorrigeerd."""
    return ModbusServerContext(slaves={unit_id: slave_context}, single=False)

last_update: dict[str, float] = {}
last_values: dict[str, float] = {}
last_update_lock = threading.Lock()
write_counter = 0


def write_value(key: str, raw_value: float) -> None:
    global write_counter
    reg_def = REGISTER_MAP.get(key)
    if reg_def is None:
        log.warning("Onbekende MQTT-key '%s', genegeerd. Controleer REGISTER_MAP.", key)
        return

    regs = value_to_registers(raw_value, reg_def)
    slave_context.setValues(3, reg_def.address, regs)  # FC3 = holding registers

    with last_update_lock:
        last_update[key] = time.monotonic()
        last_values[key] = raw_value
        write_counter += 1
    # Bewust GEEN log-regel per individuele write - bij elke waarde-update
    # loggen zou bij een paar registers om de paar seconden al snel
    # tienduizenden regels per dag opleveren. Zie start_snapshot_logger()
    # voor een periodiek, begrensd overzicht i.p.v. per-bericht logging.


# --------------------------------------------------------------------------
# MQTT
# --------------------------------------------------------------------------

def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        log.info("Verbonden met MQTT broker %s:%s", MQTT_HOST, MQTT_PORT)
        client.subscribe(f"{MQTT_BASE_TOPIC}/+")
    else:
        log.error("MQTT-verbinding mislukt, reason_code=%s", reason_code)


def on_message(client: mqtt.Client, userdata, msg):
    key = msg.topic.rsplit("/", 1)[-1]
    try:
        value = float(msg.payload.decode().strip())
    except ValueError:
        log.warning("Kon payload op topic '%s' niet als getal lezen: %r", msg.topic, msg.payload)
        return
    write_value(key, value)


def start_mqtt_thread() -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="laadpaal-rtu-slave")
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    # Last Will: als dit proces crasht of de verbinding verliest zonder
    # netjes af te sluiten, publiceert de BROKER zelf "offline" op
    # STATUS_TOPIC (retained). Dat werkt ook als het proces volledig
    # bevriest (bv. seriële poort hangt) - de broker merkt de dode
    # TCP-verbinding op via de keepalive en vuurt de LWT af.
    client.will_set(STATUS_TOPIC, payload="offline", qos=1, retain=True)

    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()
    return client


def start_heartbeat_thread(client: mqtt.Client) -> threading.Thread:
    """Publiceert periodiek 'online' (retained) zodat HA met expire_after
    kan detecteren wanneer dit proces stopt met heartbeaten - ook in
    gevallen waar de LWT niet meteen afgaat (bv. trage netwerk-timeout)."""
    def _run():
        while True:
            try:
                client.publish(STATUS_TOPIC, payload="online", qos=1, retain=True)
            except Exception:
                log.exception("Kon heartbeat niet publiceren")
            time.sleep(HEARTBEAT_INTERVAL_SECONDS)
    t = threading.Thread(target=_run, daemon=True, name="mqtt-heartbeat")
    t.start()
    return t


# --------------------------------------------------------------------------
# Watchdog: waarschuw als een waarde te lang niet vernieuwd is
# --------------------------------------------------------------------------

def start_staleness_watchdog() -> threading.Thread:
    def _run():
        while True:
            time.sleep(STALE_WARNING_SECONDS)
            now = time.monotonic()
            with last_update_lock:
                for key in REGISTER_MAP:
                    ts = last_update.get(key)
                    if ts is None:
                        log.warning("Nog geen enkele MQTT-waarde ontvangen voor '%s'", key)
                    elif now - ts > STALE_WARNING_SECONDS:
                        log.warning(
                            "Geen update voor '%s' in %.0f seconden - "
                            "controleer of Node-RED nog publiceert.",
                            key, now - ts,
                        )
    t = threading.Thread(target=_run, daemon=True, name="staleness-watchdog")
    t.start()
    return t


def start_snapshot_logger() -> threading.Thread:
    """Logt om de zoveel tijd EEN samengevatte regel met alle huidige
    waarden en het aantal verwerkte writes sinds de vorige snapshot,
    i.p.v. een regel per individuele MQTT-boodschap."""
    def _run():
        global write_counter
        while True:
            time.sleep(SNAPSHOT_LOG_INTERVAL_SECONDS)
            with last_update_lock:
                count_since_last = write_counter
                write_counter = 0
                snapshot = dict(last_values)
            log.info(
                "Snapshot (%d writes in de afgelopen %ds): %s",
                count_since_last, SNAPSHOT_LOG_INTERVAL_SECONDS, snapshot,
            )
    t = threading.Thread(target=_run, daemon=True, name="snapshot-logger")
    t.start()
    return t


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", default=SERIAL_PORT, help="Seriële poort, bv. /dev/ttyUSB0")
    parser.add_argument("--baudrate", type=int, default=BAUDRATE)
    parser.add_argument("--unit-id", type=int, default=SLAVE_UNIT_ID)
    args = parser.parse_args()

    log.info("Start MQTT-listener...")
    mqtt_client = start_mqtt_thread()

    log.info("Start heartbeat...")
    start_heartbeat_thread(mqtt_client)

    log.info("Start staleness-watchdog...")
    start_staleness_watchdog()

    log.info("Start periodieke snapshot-logger (elke %ds)...", SNAPSHOT_LOG_INTERVAL_SECONDS)
    start_snapshot_logger()

    def handle_sigterm(signum, frame):
        log.info("Signaal %s ontvangen, afsluiten...", signum)
        # Nette afsluiting: publiceer zelf al "offline" i.p.v. te wachten
        # tot de broker de LWT afvuurt na een keepalive-timeout.
        try:
            mqtt_client.publish(STATUS_TOPIC, payload="offline", qos=1, retain=True)
            time.sleep(0.2)
        except Exception:
            pass
        mqtt_client.loop_stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    log.info(
        "Start Modbus RTU-server (N380 CT register-map) op %s (baud=%d, parity=%s, unit_id=%d)...",
        args.port, args.baudrate, PARITY, args.unit_id,
    )
    server_context = build_server_context(args.unit_id)
    StartSerialServer(
        context=server_context,
        port=args.port,
        baudrate=args.baudrate,
        parity=PARITY,
        stopbits=STOPBITS,
        bytesize=BYTESIZE,
        timeout=1,
    )


if __name__ == "__main__":
    main()

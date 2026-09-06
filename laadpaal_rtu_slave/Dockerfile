FROM ghcr.io/home-assistant/base:latest

LABEL \
  io.hass.version="1.0.5" \
  io.hass.type="app" \
  io.hass.arch="aarch64|armv7"

RUN apk add --no-cache python3 py3-pip py3-wheel gcc musl-dev python3-dev libffi-dev

RUN pip3 install --no-cache-dir --break-system-packages "pymodbus==3.7.4" pyserial paho-mqtt

COPY run.sh /
COPY laadpaal_rtu_slave.py /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]

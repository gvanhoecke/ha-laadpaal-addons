ARG BUILD_FROM
FROM $BUILD_FROM

# Python + build-tools nodig om pymodbus/paho-mqtt te installeren op Alpine
RUN apk add --no-cache python3 py3-pip py3-wheel gcc musl-dev python3-dev libffi-dev

RUN pip3 install --no-cache-dir --break-system-packages pymodbus paho-mqtt

COPY run.sh /run.sh
COPY laadpaal_rtu_slave.py /laadpaal_rtu_slave.py
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]

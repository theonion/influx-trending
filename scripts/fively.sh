#!/bin/sh

for APP in avclub clickhole videohub onion;
do
    source /var/venvs/influx-trending/bin/activate && python /var/influx-trending/operations/popular.py "97.107.142.62" "8086" "root" "oaWUAkUdUu" "influxdb" "$APP.rollup.5m.percentile.99" "$APP.rollup.5m" "$APP.rollup.5m.popular.99" "10m" && deactivate
    source /var/venvs/influx-trending/bin/activate && python /var/influx-trending/operations/summation.py "97.107.142.62" "8086" "root" "oaWUAkUdUu" "influxdb" "$APP.rollup.5m" "$APP.rollup.5m.sums" "5m" && deactivate
    source /var/venvs/influx-trending/bin/activate && python /var/influx-trending/operations/trending.py "97.107.142.62" "8086" "root" "oaWUAkUdUu" "influxdb" "$APP.rollup.5m.sums" "$APP.rollup.5m.trending" "15m" && deactivate
done
exit 0

#!/bin/sh

for APP in avclub clickhole videohub onion;
do
    source /var/venvs/influx-trending/bin/activate && python /var/influx-trending/operations/popular.py "97.107.142.62" "8086" "root" "oaWUAkUdUu" "influxdb" "$APP.rollup.1h.percentile.99" "$APP.rollup.1h" "$APP.rollup.1h.popular.99" "2h" && deactivate
    source /var/venvs/influx-trending/bin/activate && python /var/influx-trending/operations/summation.py "97.107.142.62" "8086" "root" "oaWUAkUdUu" "influxdb" "$APP.rollup.1h" "$APP.rollup.1h.sums" "1h" && deactivate
    source /var/venvs/influx-trending/bin/activate && python /var/influx-trending/operations/trending.py "97.107.142.62" "8086" "root" "oaWUAkUdUu" "influxdb" "$APP.rollup.1h.sums" "$APP.rollup.1h.trending" "3h" && deactivate
done
exit 0

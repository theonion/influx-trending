#!/bin/sh

for APP in avclub clickhole videohub onion;
do
    source /var/venvs/influx-trending/bin/activate && python /var/influx-trending/operations/popular.py "97.107.142.62" "8086" "root" "oaWUAkUdUu" "influxdb" "$APP.rollup.1d.percentile.99" "$APP.rollup.1d" "$APP.rollup.1d.popular.99" "2d" && deactivate
    source /var/venvs/influx-trending/bin/activate && python /var/influx-trending/operations/summation.py "97.107.142.62" "8086" "root" "oaWUAkUdUu" "influxdb" "$APP.rollup.1d" "$APP.rollup.1d.sums" "1d" && deactivate
    source /var/venvs/influx-trending/bin/activate && python /var/influx-trending/operations/trending.py "97.107.142.62" "8086" "root" "oaWUAkUdUu" "influxdb" "$APP.rollup.1d.sums" "$APP.rollup.1d.trending" "3d" && deactivate
done
exit 0

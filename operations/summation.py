#!/usr/bin/env python3

from datetime import datetime, timedelta
import time

from influxdb import InfluxDBClient

from logger import logger


def main(client, read_series, write_series, write_rounding):

    logger.info('=== SUMMATION.PY ===')
    logger.info('{} {} {}'.format(read_series, write_series, write_rounding))

    now = datetime.now()
    offset = int(write_rounding[:-1])
    if write_rounding.endswith('m'):
        discard = timedelta(minutes=now.minute % offset, seconds=now.second, microseconds=now.microsecond)
    elif write_rounding.endswith('h'):
        discard = timedelta(
            hours=now.hour % offset, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
    elif write_rounding.endswith('d'):
        discard = timedelta(
            days=now.day % offset, hours=now.hour, minutes=now.minute, seconds=now.second,
            microseconds=now.microsecond)
    else:
        raise Exception('rounding must be done to minutes, hours or days')
    thyme = now - discard
    write_time = time.mktime(thyme.timetuple())

    content_query = 'select sum(clicks) as clicks ' \
                    'from {} ' \
                    'where content_id =~ /\d+/' \
                    'group by content_id'.format(read_series)
    results = client.query(content_query)
    if len(results):
        results = results[0]
    else:
        return None

    scrambled_write_points = []
    write_columns = ['time', 'clicks', 'content_id']
    columns = results.get('columns', [])
    points = results.get('points', [])
    for point in points:
        d = dict(zip(columns, point))
        d['time'] = write_time
        scrambled_write_points.append(d)

    write_points = []
    for d in scrambled_write_points:
        point = []
        for key in write_columns:
            point.append(d[key])
        write_points.append(point)

    write_points.sort(key=lambda p: p[1], reverse=True)
    body = [{
        'name': write_series,
        'columns': write_columns,
        'points': write_points[:100],
    }]
    logger.info(body)
    client.write_points(body)


if __name__ == '__main__':
    import sys
    # get values
    _, host, port, username, password, db, read_series, write_series, write_rounding = sys.argv

    # make client
    client = InfluxDBClient(host, port, username, password, db)

    main(client, read_series, write_series, write_rounding)

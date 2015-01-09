#!/usr/bin/env python3

from datetime import datetime, timedelta
import time

from influxdb import InfluxDBClient


def make_write_time(rounding):
    now = datetime.now()

    offset = int(rounding[:-1])

    if rounding.endswith('m'):
        discard = timedelta(minutes=now.minute % offset, seconds=now.second, microseconds=now.microsecond)
    elif rounding.endswith('h'):
        discard = timedelta(minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
    else:
        raise Exception('rounding must be done to minutes or hours')

    thyme = now - discard
    return time.mktime(thyme.timetuple())


def sum_clicks(client, series):
    query = 'select sum(clicks) as clicks from {} group by content_id'.format(series)
    results = client.query(query)
    return results


def write_sums(client, results, write_time, series):
    scrambled_write_points = []
    # write_columns = ['time', 'sequence_number', 'clicks', 'content_id']
    write_columns = ['time', 'clicks', 'content_id']

    for result in results:
        columns = result['columns']
        points = result['points']

        for point in points:
            d = dict(zip(columns, point))
            d['time'] = write_time
            # d['sequence_number'] = 1
            scrambled_write_points.append(d)

    write_points = []
    for d in scrambled_write_points:
        point = []
        for key in write_columns:
            point.append(d[key])
        write_points.append(point)

    # write_points.sort(key=lambda p: p[2], reverse=True)
    write_points.sort(key=lambda p: p[1], reverse=True)
    # for point in write_points[:100]:
    #     body = [{
    #         'name': series,
    #         'columns': write_columns,
    #         'points': [point, ],
    #     }]
    #     print(body)
    # client.write_points(body)

    body = [{
        'name': series,
        'columns': write_columns,
        'points': write_points[:100],
    }]
    print(body)
    client.write_points(body)


if __name__ == '__main__':
    import sys
    _, host, port, username, password, db, read_series, write_series, write_rounding = sys.argv

    client = InfluxDBClient(host, port, username, password, db)

    write_time = make_write_time(write_rounding)
    results = sum_clicks(client, read_series)
    write_sums(client, results, write_time, write_series)

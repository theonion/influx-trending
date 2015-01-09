#!/usr/bin/env python3

import logging
from operator import itemgetter

from influxdb import InfluxDBClient


def main(client, content_series, trending_series, offset, limit):

    query = 'select * ' \
            'from {} ' \
            'where time > now() - {} '.format(content_series, offset)
    results = client.query(query)
    if len(results):
        results = results[0]
    else:
        return None

    print(results)

    columns = results['columns']
    points = results['points']
    content = [dict(zip(columns, point)) for point in points]

    collated = {}
    for c in content:
        collated.setdefault(c['content_id'], [])
        collated[c['content_id']] = c

    content = {}
    for content_id, points in collated.items():
        points.sort(key=itemgetter('time'))
        time = points[-1]['time']



if __name__ == '__main__':
    import sys
    # get values
    _, host, port, username, password, db, content_series, trending_series, offset, limit = sys.argv

    # create client
    client = InfluxDBClient(host, port, username, password, db)

    main(client, content_series, trending_series, offset, limit)

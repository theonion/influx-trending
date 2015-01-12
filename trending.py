#!/usr/bin/env python3

from operator import itemgetter

from influxdb import InfluxDBClient

from logger import logger


def main(client, content_series, trending_series, offset):

    logger.info('=== TRENDING.PY ===')
    logger.info('{} {} {}'.format(content_series, trending_series, offset))

    query = 'select * ' \
            'from {} ' \
            'where time > now() - {} '.format(content_series, offset)
    results = client.query(query)
    if len(results):
        results = results[0]
    else:
        return None

    columns = results['columns']
    points = results['points']
    content = [dict(zip(columns, point)) for point in points]

    collated = {}
    for c in content:
        collated.setdefault(c['content_id'], [])
        collated[c['content_id']].append(c)

    columns = ['content_id', 'acceleration', 'time']
    points = []

    for content_id, results in collated.items():
        results.sort(key=itemgetter('time'))
        time = results[-1]['time']
        diff = results[-1]['clicks'] - results[0]['clicks']
        points.append([content_id, diff, time])

    body = [{
        'name': trending_series,
        'columns': columns,
        'points': points,
    }]
    logger.info(body)
    client.write_points(body)


if __name__ == '__main__':
    import sys
    # get values
    _, host, port, username, password, db, content_series, trending_series, offset = sys.argv

    # create client
    client = InfluxDBClient(host, port, username, password, db)

    main(client, content_series, trending_series, offset)

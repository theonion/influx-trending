#!/usr/bin/env python3

from influxdb import InfluxDBClient

from logger import logger


def main(client, percentile_series, content_series, popular_series, offset):

    logger.info('=== POPULAR.PY ===')
    logger.info('{} {} {} {}'.format(percentile_series, content_series, popular_series, offset))

    threshold_query = 'select * ' \
                      'from {} ' \
                      'where time > now() - {}'.format(percentile_series, offset)
    threshold_results = client.query(threshold_query)
    if len(threshold_results):
        threshold_results = threshold_results[0]
    else:
        return None

    for th_timestamp, _, threshold in threshold_results.get('points', []):
        logger.info('threshold: {}, timestamp: {}'.format(threshold, th_timestamp))
        content_query = 'select * ' \
                        'from {} ' \
                        'where clicks >= {} ' \
                        'and time > now() - {}'.format(content_series, threshold, offset)
        content_results = client.query(content_query)
        if len(content_results):
            content_results = content_results[0]
        else:
            return None

        body = [{
            'name': popular_series,
            'columns': ['time', 'clicks', 'content_id', 'threshold'],
            'points': [
                [p[0], p[2], p[3], threshold]
                for p in content_results.get('points', [])
                if p[0] == th_timestamp
            ],
        }]
        logger.info('{}'.format(body))
        client.write_points(body)


if __name__ == '__main__':
    import sys
    # get values
    _, host, port, username, password, db, percentile_series, content_series, popular_series, offset = sys.argv

    # create client
    client = InfluxDBClient(host, port, username, password, db)

    main(client, percentile_series, content_series, popular_series, offset)

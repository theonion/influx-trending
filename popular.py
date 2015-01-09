#!/usr/bin/env python3

"""
doing stuff i really wish we could have done natively in influxdb -- what a disappointment
"""

from datetime import datetime
import logging
import time

from influxdb import InfluxDBClient


def get_thresholds(client, percentile_series, time_offset):
    """gets timestamp and threshold tuples from a given series within a given offset

    :param client: an influxdb client
    :param percentile_series: the name of series containing the percentile values
    :param time_offset: the influxdb formatted time offset
    :return: a list of timestamp, threshold tuples
    """
    query = 'select * from {}'.format(percentile_series)
    results = client.query(query)

    if len(results):
        results = results[0]
    else:
        n = datetime.now()
        now = time.mktime(datetime(n.year, n.month, n.day, n.hour, 0, 0, 0).timetuple())
        results = {'points': [[now, None, 1], ]}

    points = results.get('points', [])
    return [(timestamp, threshold) for timestamp, _, threshold in points[:1]]


def get_content(client, content_series, time_offset, threshold):
    """gets content points that match a given time offset and have click values within the threshold

    :param client: an influxdb client
    :param content_series: the name of the series containing the content
    :param time_offset: the influxdb formatted time offset
    :param threshold: the minimum number of clicks allowable
    :return: a list of time, sequence_number, clicks, content_id lists
    """
    query = 'select * from {} where clicks >= {} and time > now() - {}'.format(content_series, threshold, time_offset)
    results = client.query(query)

    if len(results):
        results = results[0]
    else:
        results = {}

    points = results.get('points', [])
    return points


def write_popular_point(client, popular_series, content, threshold):
    """writes trend values to a given series so that it can be later recalled or rendered

    :param client: and influxdb client
    :param popular_series: the name of the series to write to
    :param content: the content/point lists from `get_content`
    :param threshold: the minimum number of clicks allowable
    """
    body = [{
        'name': popular_series,
        'columns': ['time', 'sequence_number', 'clicks', 'content_id', 'threshold'],
        'points': [[t[0], 1, t[2], t[3], threshold] for t in content],
    }]
    try:
        client.write_points(body)
        print(body)
    except Exception as e:
        logging.error('`write_trend_point` write operation failed: {}'.format(str(e)))


if __name__ == '__main__':
    import sys
    # get values
    _, host, port, username, password, db, percentile_series, content_series, popular_series, offset = sys.argv

    # create client
    client = InfluxDBClient(host, port, username, password, db)

    # get thresholds; iterate
    thresholds = get_thresholds(client, percentile_series, offset)
    for timestamp, threshold in thresholds:
        # log it
        logging.info('getting content: >= {} on {}'.format(threshold, timestamp))

        # get content
        content = get_content(client, content_series, offset, threshold)
        # log it
        logging.info('found {} points that match'.format(len(content)))

        # write to series
        write_popular_point(client, popular_series, content, threshold)

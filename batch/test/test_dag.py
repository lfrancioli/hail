import os
import time
import pkg_resources
import pytest
import json
import re
import requests
from flask import Response

import hailjwt as hj

from batch.client import BatchClient

from .serverthread import ServerThread


@pytest.fixture
def client():
    return BatchClient(url=os.environ.get('BATCH_URL'))


def test_user():
    fname = os.environ.get("HAIL_TOKEN_FILE")
    with open(fname) as f:
        return hj.JWTClient.unsafe_decode(f.read())


def batch_status_job_counter(batch_status, job_state):
    return len([j for j in batch_status['jobs'] if j['state'] == job_state])


def batch_status_exit_codes(batch_status):
    return [j['exit_code'] for j in batch_status['jobs']]


def test_simple(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['echo', 'head'])
    tail = batch.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[head.id])
    batch.close()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 2
    assert batch_status_exit_codes(status) == [0, 0]


def test_missing_parent_is_400(client):
    try:
        batch = client.create_batch()
        batch.create_job('alpine:3.8', command=['echo', 'head'], parent_ids=[100000])
        batch.close()
    except requests.exceptions.HTTPError as err:
        assert err.response.status_code == 400
        assert re.search('.*invalid parent_id: no job with id.*', err.response.text)
        return
    assert False


def test_dag(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['echo', 'head'])
    left = batch.create_job('alpine:3.8', command=['echo', 'left'], parent_ids=[head.id])
    right = batch.create_job('alpine:3.8', command=['echo', 'right'], parent_ids=[head.id])
    tail = batch.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[left.id, right.id])
    batch.close()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 4
    for node in [head, left, right, tail]:
        status = node.status()
        assert status['state'] == 'Complete'
        assert status['exit_code'] == 0


def test_cancel_tail(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['echo', 'head'])
    left = batch.create_job('alpine:3.8', command=['echo', 'left'], parent_ids=[head.id])
    right = batch.create_job('alpine:3.8', command=['echo', 'right'], parent_ids=[head.id])
    tail = batch.create_job(
        'alpine:3.8',
        command=['/bin/sh', '-c', 'while true; do sleep 86000; done'],
        parent_ids=[left.id, right.id])
    batch.close()
    left.wait()
    right.wait()
    batch.cancel()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 3
    for node in [head, left, right]:
        status = node.status()
        assert status['state'] == 'Complete'
        assert status['exit_code'] == 0
    assert tail.status()['state'] == 'Cancelled'


def test_cancel_left_after_tail(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['echo', 'head'])
    left = batch.create_job(
        'alpine:3.8',
        command=['/bin/sh', '-c', 'while true; do sleep 86000; done'],
        parent_ids=[head.id])
    right = batch.create_job('alpine:3.8', command=['echo', 'right'], parent_ids=[head.id])
    tail = batch.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[left.id, right.id])
    batch.close()
    head.wait()
    right.wait()
    batch.cancel()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 2
    for node in [head, right]:
        status = node.status()
        assert status['state'] == 'Complete'
        assert status['exit_code'] == 0
    for node in [left, tail]:
        assert node.status()['state'] == 'Cancelled'


def test_parent_already_done(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['echo', 'head'])
    head.wait()
    tail = batch.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[head.id])
    batch.close()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 2
    for node in [head, tail]:
        status = node.status()
        assert status['state'] == 'Complete'
        assert status['exit_code'] == 0


def test_one_of_two_parent_ids_already_done(client):
    batch = client.create_batch()
    left = batch.create_job('alpine:3.8', command=['echo', 'left'])
    left.wait()
    right = batch.create_job('alpine:3.8', command=['echo', 'right'])
    tail = batch.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[left.id, right.id])
    batch.close()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 3
    for node in [left, right, tail]:
        status = node.status()
        assert status['state'] == 'Complete'
        assert status['exit_code'] == 0


def test_callback(client):
    from flask import Flask, request
    app = Flask('test-client')
    output = []

    @app.route('/test', methods=['POST'])
    def test():
        output.append(request.get_json())
        return Response(status=200)

    try:
        server = ServerThread(app)
        server.start()
        batch = client.create_batch(callback=server.url_for('/test'))
        head = batch.create_job('alpine:3.8', command=['echo', 'head'])
        left = batch.create_job('alpine:3.8', command=['echo', 'left'], parent_ids=[head.id])
        right = batch.create_job('alpine:3.8', command=['echo', 'right'], parent_ids=[head.id])
        tail = batch.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[left.id, right.id])
        batch.close()
        batch.wait()
        i = 0
        while len(output) != 4:
            time.sleep(0.100 * (3/2) ** i)
            i += 1
            if i > 14:
                break
        assert len(output) == 4
        assert all([job_result['state'] == 'Complete' and job_result['exit_code'] == 0
                    for job_result in output])
        assert output[0]['id'] == head.id
        middle_ids = (output[1]['id'], output[2]['id'])
        assert middle_ids in ((left.id, right.id), (right.id, left.id))
        assert output[3]['id'] == tail.id
    finally:
        if server:
            server.shutdown()
            server.join()


def test_from_file(client):
        fname = pkg_resources.resource_filename(
            __name__,
            'diamond_dag.yml')
        with open(fname) as f:
            batch = client.create_batch_from_file(f)

        batch.close()
        status = batch.wait()
        assert batch_status_job_counter(status, 'Complete') == 4


def test_no_parents_allowed_in_other_batches(client):
    b1 = client.create_batch()
    b2 = client.create_batch()
    head = b1.create_job('alpine:3.8', command=['echo', 'head'])
    try:
        b2.create_job('alpine:3.8', command=['echo', 'tail'], parent_ids=[head.id])
    except requests.exceptions.HTTPError as err:
        assert err.response.status_code == 400
        assert re.search('.*invalid parent batch: .*', err.response.text)
        return
    assert False


def test_input_dependency(client):
    user = test_user()
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8',
                            command=['/bin/sh', '-c', 'echo head1 > /io/data1 ; echo head2 > /io/data2'],
                            output_files=[('/io/data*', f'gs://{user["bucket_name"]}')])
    tail = batch.create_job('alpine:3.8',
                            command=['/bin/sh', '-c', 'cat /io/data1 ; cat /io/data2'],
                            input_files=[(f'gs://{user["bucket_name"]}/data\\*', '/io/')],
                            parent_ids=[head.id])
    batch.close()
    tail.wait()
    assert head.status()['exit_code'] == 0, head.cached_status()
    assert tail.log()['main'] == 'head1\nhead2\n'


def test_input_dependency_directory(client):
    user = test_user()
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8',
                            command=['/bin/sh', '-c', 'mkdir -p /io/test/; echo head1 > /io/test/data1 ; echo head2 > /io/test/data2'],
                            output_files=[('/io/test/', f'gs://{user["bucket_name"]}')])
    tail = batch.create_job('alpine:3.8',
                            command=['/bin/sh', '-c', 'cat /io/test/data1 ; cat /io/test/data2'],
                            input_files=[(f'gs://{user["bucket_name"]}/test', '/io/')],
                            parent_ids=[head.id])
    batch.close()
    tail.wait()
    assert head.status()['exit_code'] == 0, head.cached_status()
    assert tail.log()['main'] == 'head1\nhead2\n', tail.log()


def test_always_run_cancel(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['echo', 'head'])
    left = batch.create_job(
        'alpine:3.8',
        command=['/bin/sh', '-c', 'while true; do sleep 86000; done'],
        parent_ids=[head.id])
    right = batch.create_job('alpine:3.8', command=['echo', 'right'], parent_ids=[head.id])
    tail = batch.create_job('alpine:3.8',
                            command=['echo', 'tail'],
                            parent_ids=[left.id, right.id],
                            always_run=True)
    batch.close()
    right.wait()
    batch.cancel()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 3
    for node in [head, right, tail]:
        status = node.status()
        assert status['state'] == 'Complete'
        assert status['exit_code'] == 0


def test_always_run_error(client):
    batch = client.create_batch()
    head = batch.create_job('alpine:3.8', command=['/bin/sh', '-c', 'exit 1'])
    tail = batch.create_job('alpine:3.8',
                            command=['echo', 'tail'],
                            parent_ids=[head.id],
                            always_run=True)
    batch.close()
    status = batch.wait()
    assert batch_status_job_counter(status, 'Complete') == 2

    for job, ec in [(head, 1), (tail, 0)]:
        status = job.status()
        assert status['state'] == 'Complete'
        assert status['exit_code'] == ec

#!/usr/bin/python

import json
import kubernetes
import os
import requests
import sys
import time
import traceback

from HmfApi import *
from datetime import datetime

def phone_home(message):
    print message

    payload = {"text": '```' + message + '```'}
    requests.post(
        os.environ['SLACK_HOOK'],
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json'}
    )


def log(msg):
    print (time.strftime('[%Y-%m-%d %H:%M:%S] ') + str(msg))


def start_kubernetes_job(args):
    kubernetes.config.load_incluster_config()

    timestamp = time.strftime('%Y%m%d%H%M%S')
    with file('/var/run/secrets/kubernetes.io/serviceaccount/namespace') as f:
        namespace = f.read()

    job_args = ['-sbp_run_id', str(args['sbp_run_id'])]

    for i in range(1, len(sys.argv)):
        job_args.append(sys.argv[i])

    spec = kubernetes.client.V1Job(
        metadata=kubernetes.client.V1ObjectMeta(
            name='pipelinev5-somatic-{0}-{1}'.format(args['sbp_run_id'], timestamp)
        ),
        spec=kubernetes.client.V1JobSpec(
            completions=1,
            parallelism=1,
            backoff_limit=3,
            template=kubernetes.client.V1PodTemplateSpec(
                spec=kubernetes.client.V1PodSpec(
                    restart_policy='Never',
                    containers=[
                        kubernetes.client.V1Container(
                            name='pipelinev5-somatic-{0}-{1}'.format(args['sbp_run_id'], timestamp),
                            image='hartwigmedicalfoundation/pipeline5:{0}'.format(os.environ['PIPELINE_VERSION']),
                            command=[
                                '/pipeline5.sh'
                            ],
                            args=job_args,
                            env=[
                                kubernetes.client.V1EnvVar(
                                    name='READER_ACL_IDS',
                                    value='0403732075957f94c7baea5ad60b233f,f39de0aec3c8b5bb9d78a22ad88428ad'
                                ),
                                kubernetes.client.V1EnvVar(
                                    name='READER_ACP_ACL_IDS',
                                    value='0403732075957f94c7baea5ad60b233f'
                                )
                            ],
                            volume_mounts=[
                                kubernetes.client.V1VolumeMount(
                                    name='hmf-upload-credentials',
                                    mount_path='/root/.aws'
                                ),
                                kubernetes.client.V1VolumeMount(
                                    name='gcp-pipeline5-scheduler',
                                    mount_path='/secrets/'
                                ),
                                kubernetes.client.V1VolumeMount(
                                    name='rclone-config',
                                    mount_path='/root/.config/rclone'
                                )
                            ],
                            resources=kubernetes.client.V1ResourceRequirements(
                                requests={
                                    'memory': '2Gi'
                                }
                            )
                        )
                    ],
                    volumes=[
                        kubernetes.client.V1Volume(
                            name='hmf-upload-credentials',
                            secret=kubernetes.client.V1SecretVolumeSource(
                                secret_name='hmf-upload-credentials'
                            )
                        ),
                        kubernetes.client.V1Volume(
                            name='gcp-pipeline5-scheduler',
                            secret=kubernetes.client.V1SecretVolumeSource(
                                secret_name='gcp-pipeline5-scheduler'
                            )
                        ),
                        kubernetes.client.V1Volume(
                            name='rclone-config',
                            secret=kubernetes.client.V1SecretVolumeSource(
                                secret_name='rclone-config'
                            )
                        )
                    ]
                )
            )
        )
    )

    job = kubernetes. \
        client. \
        BatchV1Api(). \
        create_namespaced_job(
            namespace,
            spec
        )

    log('Started job {0}'.format(job.metadata.name))
    return job


def main():
    ini = Ini().get_one({'name': 'PipelineV5.ini'})
    stack = Stack().get_one({'name': 'Google Compute Platform'})
    runs = HmfApi().get_all(Run, {'status': 'Pending', 'ini_id': ini.id})

    max_starts = int(os.getenv('MAX_STARTS', '4'))

    if len(runs) > 0:
        log('Scheduling {0} out of {1} somatic runs'.format(max_starts,len(runs)))
        del(runs[max_starts:])

        for run in runs:
            start_kubernetes_job({'sbp_run_id': run.id})

            phone_home('Starting Pipeline {0} for run {1}'.format(os.environ['PIPELINE_VERSION'], run.id))

            run.status = 'Processing'
            run.stack_id = stack.id
            run.startTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            run.save()
    else:
        log('No Pipeline v5 runs currently ready to go')


try:
    main()
except BaseException, e:
    message = "Exception in pipeline5 pending-check-somatic\n\n"
    message += str(e) + "\n\n"
    message += traceback.format_exc()
    message += "\nWill now sleep until human takes a look"

    phone_home(message)

    while True:
        time.sleep(60)

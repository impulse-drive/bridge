const QUEUE_URL = process.env.QUEUE_URL;
const NAMESPACE = process.env.NAMESPACE || 'default';

if(!QUEUE_URL) {
    console.error(`QUEUE_URL=${QUEUE_URL}`);
    process.exit(1);
}

if(!/^[a-zA-z0-9\-]+$/.test(NAMESPACE)) {
    console.error(`NAMESPACE=${NAMESPACE}: bad namespace`);
    process.exit(1);
}

const k8s = new (require('kubernetes-client').Client)({ version: '1.13' });
const nats = require('nats').connect(QUEUE_URL, { json: true });
nats._publish = nats.publish;
nats.publish = (topic, payload) => {
    console.log({[topic]: payload});
    nats._publish(topic, payload);
};

nats.subscribe('pipeline.*.job.*.task.*.start', async (msg, reply, subject, sid) => {
    // IMPORTANT: Requires feature gate TTLAfterFinished, run minikube
    //            with --feature-gates=TTLAfterFinished=true
    const { image, command, args, build, output, resources } = msg;
    const [,pipeline,,job,,task,] = subject.split('.');

    console.log({ msg, reply, subject, sid});
    console.log(msg);

    const name = `pipeline.${pipeline}.job.${job}.task.${task}.${build}`;

    const postPayload = {
        body: {
            apiVersion: 'batch/v1',
            kind: 'Job',
            metadata: { name },
            spec: {
                ttlSecondsAfterFinished: 120, //TTLAfterFinished
                backoffLimit: 0,
                template: {
                    metadata: {
                        labels: { task: name }
                    },
                    spec: {
                        containers: [
                            {
                                name: name.replace(/\./g,'-'),
                                image: 'aerkenemesis/worker:latest',
                                args: [
                                    //'docker',
                                    //'run',
                                    //'--rm',
                                    //`-v${name.replace(/\./g,'-')}-runtime:/runtime`,
                                    //`-v${name.replace(/\./g,'-')}-output:/output`,
                                    //'--volumes-from',
                                    //'$(docker inspect --format="{{.Id}}" ' + name.replace(/\./g,'-') + ')',
                                    //'-w/runtime',
                                    image,
                                    command,
                                    ...args
                                ],
                                imagePullPolicy: 'Always',
                                env: [
                                    {
                                        name: 'NAME',
                                        value: name,
                                    },
                                    {
                                        name: 'CONTAINER',
                                        value: name.replace(/\./g,'-')
                                    },
                                    {
                                        name: 'QUEUE_URL',
                                        value: QUEUE_URL
                                    },
                                    {
                                        name: 'RUNTIME_DIR',
                                        value: '/runtime'
                                    },
                                    {
                                        name: 'OUTPUT_DIR',
                                        value: '/output'
                                    },
                                    {
                                        name: 'BUILD',
                                        value: `${build}`
                                    },
                                    {
                                        name: 'OUTPUT',
                                        value: `${output.bucket}|${output.object}`
                                    },
                                    {
                                        name: 'INPUTS',
                                        value: Object.entries(resources).map(([k,v]) => `${k}|${v.bucket}|${v.object}`).join('|')
                                    },
                                    {
                                        name: 'MINIO_ACCESS_KEY',
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: 'impulse-drive-minio',
                                                key: 'access-key'
                                            }
                                        }

                                    },
                                    {
                                        name: 'MINIO_SECRET_KEY',
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: 'impulse-drive-minio',
                                                key: 'secret-key'
                                            }
                                        }
                                    }
                                ],
                                envFrom: [
                                    {
                                        configMapRef: {
                                            name: 'impulse-drive-services'
                                        }
                                    }
                                ],
                                volumeMounts: [
                                    {
                                        name: `${name.replace(/\./g,'-')}-runtime`,
                                        mountPath: '/runtime'
                                    },
                                    {
                                        name: `${name.replace(/\./g,'-')}-output`,
                                        mountPath: '/output'
                                    },
                                    {
                                        name: 'docker-socket',
                                        mountPath: '/var/run/docker.sock'
                                    }
                                ],
                                securityContext: {
                                    privileged: true
                                }
                            }
                        ],
                        volumes: [
                            {
                                name: `${name.replace(/\./g,'-')}-runtime`,
                                emptyDir: {}
                            },
                            {
                                name: `${name.replace(/\./g,'-')}-output`,
                                emptyDir: {}
                            },
                            {
                                name: 'docker-socket',
                                hostPath: {
                                    path: '/var/run/docker.sock',
                                    type: 'File'
                                }
                            }
                        ],
                        restartPolicy: 'Never'
                    }
                }
            }
        }
    };

    const publish = msg => nats.publish(name, {...msg, build});

    const error = error => {
        if(reply) {
            nats.publish(reply, {status: 'failed'});
        }
        publish({status: 'failed', error});
    };

    const onAdded = async data => publish({status: 'pending'});

    const onActive = async data => publish({status: 'running'});

    const onFailed = async data => error();

    const onSucceeded = async data => {
        if(reply) {
            nats.publish(reply, {status: 'succeeded'});
        }
        publish({status: 'succeeded'});
    };

    k8s.apis.batch.v1.watch.namespaces(NAMESPACE).jobs.getObjectStream()
        .then(async stream => {
            stream.on('data', async data => {
                console.log(data);
                if(data.object.metadata.name != postPayload.body.metadata.name) {
                    return
                }
                if(data.type == 'ADDED') {
                    await onAdded(data);
                    return;
                }
                if(data.type == 'MODIFIED' && data.object.status.active) {
                    await onActive(data);
                    return;
                }
                if(data.type == 'MODIFIED' && data.object.status.succeeded) {
                    await onSucceeded(data);
                    stream.destroy();
                    return;
                }
                if(data.type == 'MODIFIED' && data.object.status.failed) {
                    await onFailed(data);
                    stream.destroy();
                    return;
                }
            });
            k8s.apis.batch.v1.namespace(NAMESPACE).job
                .post(postPayload)
                .catch(error);
        })
        .catch(error);
});

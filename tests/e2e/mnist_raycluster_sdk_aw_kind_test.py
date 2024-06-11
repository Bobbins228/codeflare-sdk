import requests

from time import sleep

from codeflare_sdk import Cluster, ClusterConfiguration, TokenAuthentication
from codeflare_sdk.job import RayJobClient

import pytest

from support import *
from time import sleep

# This test creates an AppWrapper containing a Ray Cluster and covers the Ray Job submission functionality on Kind Cluster


@pytest.mark.kind
class TestRayClusterSDKAppWrapperKind:
    def setup_method(self):
        initialize_kubernetes_client(self)

    def teardown_method(self):
        delete_namespace(self)

    def test_mnist_ray_cluster_sdk_kind(self):
        self.setup_method()
        create_namespace(self)
        create_kueue_resources(self)
        self.run_mnist_raycluster_sdk_kind()

    def run_mnist_raycluster_sdk_kind(self):
        ray_image = get_ray_image()

        cluster = Cluster(
            ClusterConfiguration(
                name="mnist",
                namespace=self.namespace,
                num_workers=1,
                head_cpus="500m",
                head_memory=2,
                min_cpus="500m",
                max_cpus=1,
                min_memory=1,
                max_memory=2,
                num_gpus=0,
                image=ray_image,
                write_to_file=True,
                verify_tls=False,
                appwrapper=True,
            )
        )

        cluster.up()
        sleep(45)
        api_instance = client.CustomObjectsApi()
        rcs = api_instance.list_namespaced_custom_object(
            group="ray.io",
            version="v1",
            namespace=self.namespace,
            plural="rayclusters",
        )
        print("------------------ Ray Cluster ------------------")
        for rc in rcs["items"]:
            print(rc)
        print("------------------ EVENTS ------------------")
        v1 = client.CoreV1Api()
        print(f"Events in namespace: {self.namespace}")
        try:
            events = v1.list_namespaced_event(namespace=self.namespace).items
            for event in events:
                print(
                    f"Event: {event.metadata.name}, Reason: {event.reason}, Message: {event.message}, Timestamp: {event.last_timestamp}"
                )
        except client.exceptions.ApiException as e:
            print(f"Exception when calling CoreV1Api->list_namespaced_event: {e}")

        print("------------------ Workloads ------------------")
        api_instance = client.CustomObjectsApi()
        try:
            workloads = api_instance.list_namespaced_custom_object(
                "kueue.x-k8s.io", "v1beta1", self.namespace, "workloads"
            )
            for workload in workloads.get("items", []):
                name = workload["metadata"]["name"]
                status = workload.get("status", {})
                print(
                    f"Workload: {name}, Namespace: {self.namespace}, Status: {status}"
                )
        except client.exceptions.ApiException as e:
            print(
                f"Exception when calling CustomObjectsApi->list_namespaced_custom_object: {e}"
            )
        cluster.status()

        cluster.wait_ready()

        cluster.status()

        cluster.details()

        self.assert_jobsubmit_withoutlogin_kind(cluster)

    # Assertions

    def assert_jobsubmit_withoutlogin_kind(self, cluster):
        ray_dashboard = cluster.cluster_dashboard_uri()
        client = RayJobClient(address=ray_dashboard, verify=False)

        submission_id = client.submit_job(
            entrypoint="python mnist.py",
            runtime_env={
                "working_dir": "./tests/e2e/",
                "pip": "./tests/e2e/mnist_pip_requirements.txt",
            },
        )
        print(f"Submitted job with ID: {submission_id}")
        done = False
        time = 0
        timeout = 900
        while not done:
            status = client.get_job_status(submission_id)
            if status.is_terminal():
                break
            if not done:
                print(status)
                if timeout and time >= timeout:
                    raise TimeoutError(f"job has timed out after waiting {timeout}s")
                sleep(5)
                time += 5

        logs = client.get_job_logs(submission_id)
        print(logs)

        self.assert_job_completion(status)

        client.delete_job(submission_id)

        cluster.down()

    def assert_job_completion(self, status):
        if status == "SUCCEEDED":
            print(f"Job has completed: '{status}'")
            assert True
        else:
            print(f"Job has completed: '{status}'")
            assert False

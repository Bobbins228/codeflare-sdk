from codeflare_sdk import (
    Cluster,
    ClusterConfiguration,
    TokenAuthentication,
    generate_cert,
)

import pytest
import ray
import math
from time import sleep

from support import *


@pytest.mark.kind
class TestRayLocalInteractiveOauth:
    def setup_method(self):
        initialize_kubernetes_client(self)

    def teardown_method(self):
        delete_namespace(self)
        delete_kueue_resources(self)

    def test_local_interactives(self):
        self.setup_method()
        create_namespace(self)
        create_kueue_resources(self)
        self.run_local_interactives()

    def run_local_interactives(self):
        ray_image = get_ray_image()

        cluster_name = "test-ray-cluster-li"

        cluster = Cluster(
            ClusterConfiguration(
                name=cluster_name,
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
                write_to_file=False,
                verify_tls=False,
            )
        )
        cluster.up()
        sleep(60)
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

        cluster.wait_ready()

        generate_cert.generate_tls_cert(cluster_name, self.namespace)
        generate_cert.export_env(cluster_name, self.namespace)

        print(cluster.local_client_url())

        ray.shutdown()
        ray.init(address=cluster.local_client_url(), logging_level="DEBUG")

        @ray.remote
        def heavy_calculation_part(num_iterations):
            result = 0.0
            for i in range(num_iterations):
                for j in range(num_iterations):
                    for k in range(num_iterations):
                        result += math.sin(i) * math.cos(j) * math.tan(k)
            return result

        @ray.remote
        def heavy_calculation(num_iterations):
            results = ray.get(
                [heavy_calculation_part.remote(num_iterations // 30) for _ in range(30)]
            )
            return sum(results)

        ref = heavy_calculation.remote(3000)
        result = ray.get(ref)
        assert result == 1789.4644387076714
        ray.cancel(ref)
        ray.shutdown()

        cluster.down()

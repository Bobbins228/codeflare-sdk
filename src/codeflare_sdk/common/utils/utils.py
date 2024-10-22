# Copyright 2024 IBM, Red Hat
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from ..kubernetes_cluster.kube_api_helpers import _kube_api_error_handling
from ..kubernetes_cluster.auth import config_check
from kubernetes import config
import os


def get_current_namespace():  # pragma: no cover
    if os.path.isfile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"):
        try:
            file = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r")
            active_context = file.readline().strip("\n")
            return active_context
        except Exception as e:
            print("Unable to find current namespace")
    print("trying to gather from current context")
    try:
        _, active_context = config.list_kube_config_contexts(config_check())
    except Exception as e:
        return _kube_api_error_handling(e)
    try:
        return active_context["context"]["namespace"]
    except KeyError:
        return None

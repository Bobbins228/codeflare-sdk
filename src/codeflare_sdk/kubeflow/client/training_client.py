from kubeflow.training.api.training_client import TrainingClient as tc
from kubeflow.training.api_client import ApiClient
from kubeflow.training.constants import constants
from kubeflow.training import models

from ...common.utils.utils import get_current_namespace
from ...common.kubernetes_cluster.auth import config_check, get_api_client
from kubernetes import client
from typing import Optional, Union, Dict, List, Callable, Any, Set, Tuple


class TrainingClient:
    def __init__(
        self,
        config_file: Optional[str] = None,
        context: Optional[str] = None,
        client_configuration: Optional[client.Configuration] = None,
        namespace: str = get_current_namespace(),
        job_kind: str = constants.PYTORCHJOB_KIND,
    ):
        config_check()
        if (
            get_api_client() != None
        ):  # Can save the user from passing config themselves using get_api_client
            client_configuration = get_api_client().configuration

        self.trainingClient = tc(
            client_configuration=client_configuration,
            config_file=config_file,
            context=context,
            namespace=namespace,
            job_kind=job_kind,
        )

    def train(
        self,
        name: str,
        namespace: Optional[str] = None,
        num_workers: int = 1,
        num_procs_per_worker: int = 1,
        resources_per_worker: Union[dict, client.V1ResourceRequirements, None] = None,
        model_provider_parameters=None,
        dataset_provider_parameters=None,
        trainer_parameters=None,
        storage_config: Dict[str, Optional[Union[str, List[str]]]] = {
            "size": constants.PVC_DEFAULT_SIZE,
            "storage_class": None,
            "access_modes": constants.PVC_DEFAULT_ACCESS_MODES,
        },
    ):
        self.trainingClient.train(
            name,
            namespace,
            num_workers,
            num_procs_per_worker,
            resources_per_worker,
            model_provider_parameters,
            dataset_provider_parameters,
            trainer_parameters,
            storage_config,
        )

    def create_job(
        self,
        job: Optional[constants.JOB_MODELS_TYPE] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        base_image: Optional[str] = None,
        train_func: Optional[Callable] = None,
        parameters: Optional[Dict[str, Any]] = None,
        num_workers: Optional[int] = None,
        resources_per_worker: Union[dict, models.V1ResourceRequirements, None] = None,
        num_chief_replicas: Optional[int] = None,
        num_ps_replicas: Optional[int] = None,
        packages_to_install: Optional[List[str]] = None,
        pip_index_url: str = constants.DEFAULT_PIP_INDEX_URL,
    ):
        self.trainingClient.create_job(
            job,
            name,
            namespace,
            job_kind,
            base_image,
            train_func,
            parameters,
            num_workers,
            resources_per_worker,
            num_chief_replicas,
            num_ps_replicas,
            packages_to_install,
            pip_index_url,
        )

    def get_job(
        self,
        name: str,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        timeout: int = constants.DEFAULT_TIMEOUT,
    ) -> constants.JOB_MODELS_TYPE:
        return self.trainingClient.get_job(name, namespace, job_kind, timeout)

    def list_jobs(
        self,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        timeout: int = constants.DEFAULT_TIMEOUT,
    ) -> List[constants.JOB_MODELS_TYPE]:
        return self.trainingClient.list_jobs(namespace, job_kind, timeout)

    def get_job_conditions(
        self,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        job: Optional[constants.JOB_MODELS_TYPE] = None,
        timeout: int = constants.DEFAULT_TIMEOUT,
    ) -> List[models.V1JobCondition]:
        return self.trainingClient.get_job_conditions(
            name, namespace, job_kind, job, timeout
        )

    def is_job_created(
        self,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        job: Optional[constants.JOB_MODELS_TYPE] = None,
        timeout: int = constants.DEFAULT_TIMEOUT,
    ) -> bool:
        return self.trainingClient.is_job_created(
            name, namespace, job_kind, job, timeout
        )

    def is_job_running(
        self,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        job: Optional[constants.JOB_MODELS_TYPE] = None,
        timeout: int = constants.DEFAULT_TIMEOUT,
    ) -> bool:
        return self.trainingClient.is_job_running(
            name, namespace, job_kind, job, timeout
        )

    def is_job_restarting(
        self,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        job: Optional[constants.JOB_MODELS_TYPE] = None,
        timeout: int = constants.DEFAULT_TIMEOUT,
    ) -> bool:
        return self.trainingClient.is_job_restarting(
            name, namespace, job_kind, job, timeout
        )

    def is_job_succeeded(
        self,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        job: Optional[constants.JOB_MODELS_TYPE] = None,
        timeout: int = constants.DEFAULT_TIMEOUT,
    ) -> bool:
        return self.trainingClient.is_job_succeeded(
            name, namespace, job_kind, job, timeout
        )

    def is_job_failed(
        self,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        job: Optional[constants.JOB_MODELS_TYPE] = None,
        timeout: int = constants.DEFAULT_TIMEOUT,
    ) -> bool:
        return self.trainingClient.is_job_failed(
            name, namespace, job_kind, job, timeout
        )

    def wait_for_job_conditions(
        self,
        name: str,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        expected_conditions: Set = {constants.JOB_CONDITION_SUCCEEDED},
        wait_timeout: int = 600,
        polling_interval: int = 15,
        callback: Optional[Callable] = None,
        timeout: int = constants.DEFAULT_TIMEOUT,
    ) -> constants.JOB_MODELS_TYPE:
        return self.trainingClient.wait_for_job_conditions(
            name,
            namespace,
            job_kind,
            expected_conditions,
            wait_timeout,
            polling_interval,
            callback,
            timeout,
        )

    def get_job_pods(
        self,
        name: str,
        namespace: Optional[str] = None,
        is_master: bool = False,
        replica_type: Optional[str] = None,
        replica_index: Optional[int] = None,
        timeout: int = constants.DEFAULT_TIMEOUT,
    ) -> List[models.V1Pod]:
        return self.trainingClient.get_job_pods(
            name, namespace, is_master, replica_type, replica_index, timeout
        )

    def get_job_pod_names(
        self,
        name: str,
        namespace: Optional[str] = None,
        is_master: bool = False,
        replica_type: Optional[str] = None,
        replica_index: Optional[int] = None,
        timeout: int = constants.DEFAULT_TIMEOUT,
    ) -> List[str]:
        return self.trainingClient.get_job_pod_names(
            name, namespace, is_master, replica_type, replica_index, timeout
        )

    def get_job_logs(
        self,
        name: str,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        is_master: bool = True,
        replica_type: Optional[str] = None,
        replica_index: Optional[int] = None,
        follow: bool = False,
        timeout: int = constants.DEFAULT_TIMEOUT,
        verbose: bool = False,
    ) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
        return self.trainingClient.get_job_logs(
            name,
            namespace,
            job_kind,
            is_master,
            replica_type,
            replica_index,
            follow,
            timeout,
            verbose,
        )

    def update_job(
        self,
        job: constants.JOB_MODELS_TYPE,
        name: str,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
    ):
        self.trainingClient.update_job(job, name, namespace, job_kind)

    def delete_job(
        self,
        name: str,
        namespace: Optional[str] = None,
        job_kind: Optional[str] = None,
        delete_options: Optional[models.V1DeleteOptions] = None,
    ):
        self.trainingClient.delete_job(name, namespace, job_kind, delete_options)

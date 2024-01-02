from airflow import AirflowException
from airflow.hooks.base import BaseHook
import logging
from utils.gitlab import Gitlab


class GitlabHook(BaseHook):
    """A hook that provides an interface to the Gitlab API.

    This hook inherits from the BaseHook class and uses the Gitlab class from the utils.gitlab module to make HTTP requests to the Gitlab API.

    :param gitlab_conn_id: The connection ID to use for the hook.
    :type gitlab_conn_id: str
    """

    conn_name_attr = "gitlab_conn_id"
    default_conn_name = "gitlab_default"
    conn_type = "gitlab"
    hook_name = "Gitlab"

    def __init__(self, gitlab_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.gitlab_conn_id = gitlab_conn_id
        self.client: Gitlab | None = None
        self.get_conn()

    def get_conn(self) -> Gitlab:
        """Gets the Gitlab object to interact with the API.

        This method creates a new Gitlab object if it does not exist, or returns the existing one. It uses the connection details from the Airflow connection database to initialize the Gitlab object.

        :return: The Gitlab object to interact with the API.
        :rtype: Gitlab

        :raise: If the connection details are missing or invalid.
        """
        logger = logging.getLogger(__name__)
        if self.client is not None:
            return self.client

        conn = self.get_connection(self.gitlab_conn_id)
        access_token = conn.password
        host = conn.host

        if not access_token:
            raise AirflowException("An access token is required to authenticate to Gitlab.")

        if not host:
            raise AirflowException("Host is required to connect to Gitlab.")

        self.client = Gitlab(url=host, private_token=access_token)
        logger.info("Gitlab is initialized for host {}".format(host))
        return self.client


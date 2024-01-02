import logging
from typing import Any, Dict, List

from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults

from hooks.gitlab_hook import GitlabHook
from triggers.gitlab_trigger import GitlabRepoChangedTrigger


class GitLabRepoChangeDetectionSensor(BaseSensorOperator):
    """
    An Airflow sensor that detects changes in GitLab repositories.

    :param gitlab_conn_id: The connection ID for GitLab.
    :type gitlab_conn_id: str
    :param project_ids: A dictionary of project IDs and their corresponding branch names.
    :type project_ids: Dict[int, str]
    :param check_since: The timestamp to check changes since.
    :type check_since: str
    """

    @apply_defaults
    def __init__(self,
                 *,
                 gitlab_conn_id: str,
                 project_ids: Dict[int, str],
                 check_since: str,
                 **kwargs):
        super().__init__(**kwargs)
        self.project_ids = project_ids
        self.gitlab_conn_id = gitlab_conn_id
        self.check_since = check_since
        self.__log = logging.getLogger(__name__)

    def poke(self, context):
        """
        Check for changes in GitLab repositories.

        :param context: The execution context.
        :type context: dict
        :return: True if changes are detected, False otherwise.
        :rtype: bool
        """
        hook = GitlabHook(self.gitlab_conn_id)
        self.__log.info("Checking for changes in GitLab repo with id {} since {}".format(self.project_ids, context[
            "data_interval_start"]))
        changed_repos = []

        for project_id, branch in self.project_ids.items():
            commits = hook.get_conn().sync_get_commits(project_id=project_id, branch_name=branch,
                                                       since=str(context["data_interval_start"]))
            if commits.success and len(commits.commits) > 0:
                changed_repos.append(project_id)

        context["ti"].xcom_push("changed_repos", changed_repos)
        return len(changed_repos) > 0


class AwaitGitLabRepoChangeDetectionSensor(BaseOperator):
    """
    An Airflow deferrable operator that awaits GitLab repository change detection.

    :param gitlab_conn_id: The connection ID for GitLab.
    :type gitlab_conn_id: str
    :param projects: A dictionary containing project details.
    :type projects: Dict
    :param check_runs: The number of runs to check for changes.
    :type check_runs: int
    :param check_interval: The interval between checks in seconds.
    :type check_interval: int
    :param xcom_push_key: The key for XCom push. Defaults to 'changed_repos' if not provided.
    :type xcom_push_key: str
    """

    @apply_defaults
    def __init__(self,
                 gitlab_conn_id: str,
                 projects: Dict,
                 check_runs: int = 10,
                 check_interval: int = 60,
                 xcom_push_key: str = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.gitlab_conn_id = gitlab_conn_id
        self.projects = projects
        self.check_runs = check_runs
        self.check_interval = check_interval
        self.xcom_push_key = xcom_push_key if xcom_push_key else 'changed_repos'

    def execute(self, context: Context) -> Any:
        """
        Execute the GitLab repository change detection trigger.

        :param context: The execution context.
        :type context: dict
        :return: The trigger event.
        :rtype: Any
        """
        self.defer(
            trigger=GitlabRepoChangedTrigger(
                projects=self.projects,
                changes_since=str(context["data_interval_start"]),
                check_runs=self.check_runs,
                check_interval=self.check_interval,
                gitlab_conn_id=self.gitlab_conn_id),
            method_name='execute_completed')

    def execute_completed(self, context, event=None):
        """
        Complete execution of the trigger and push the event to XCom.

        :param context: The execution context.
        :type context: dict
        :param event: The trigger event.
        :type event: Any
        :return: The trigger event.
        :rtype: Any
        """
        self.xcom_push(context, key=self.xcom_push_key, value=event)
        return event

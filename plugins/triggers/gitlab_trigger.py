import asyncio
from typing import Any, Tuple, Dict

from airflow.triggers.base import BaseTrigger, TriggerEvent
import logging
from hooks.gitlab_hook import GitlabHook


class GitlabRepoChangedTrigger(BaseTrigger):
    """A trigger that checks for changes in Gitlab repositories.

    This trigger polls the Gitlab API for commits in specified projects and branches since a given date.
    It yields a TriggerEvent with a list of changed project IDs if any changes are detected or the maximum number of runs is reached.

    :param gitlab_conn_id: The connection ID to use for the Gitlab hook.
    :type gitlab_conn_id: str
    :param projects: A dictionary of project IDs and branch names to monitor.
    :type projects: dict
    :param changes_since: The date and time to start checking for changes, in ISO 8601 format.
    :type changes_since: str
    :param check_runs: The maximum number of times to run the trigger before yielding an event.
    :type check_runs: int
    :param check_interval (int): The number of seconds to wait between each run.
    :type check_interval: int
    """

    def __init__(self, gitlab_conn_id: str,
                 projects: Dict,
                 changes_since: str,
                 check_runs: int,
                 check_interval: int) -> None:
        super().__init__()
        self.gitlab_conn_id = gitlab_conn_id
        self.projects = projects
        self.changes_since = changes_since
        self.check_runs = check_runs
        self.check_interval = check_interval
        self.runs = 0

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "triggers.gitlab_trigger.GitlabRepoChangedTrigger",
            {
                "gitlab_conn_id": self.gitlab_conn_id,
                "projects": self.projects,
                "changes_since": self.changes_since,
                "check_runs": self.check_runs,
                "check_interval": self.check_interval
            },
        )

    async def run(self):
        """Runs the trigger and checks for changes in Gitlab repositories.

        This method is a coroutine that runs in an infinite loop until either changes are detected or the maximum number of runs is reached.
        It uses the GitlabHook to get a connection to the Gitlab API and polls for commits in the specified
        projects and branches. It logs the results of each check and appends the project IDs of the changed repositories to a list.
        It yields a TriggerEvent with the list of changed project IDs as the payload when the loop ends.

        :return: A TriggerEvent with the list of changed project IDs as the payload.
        :rtype: TriggerEvent
        """
        log = logging.getLogger(__name__)
        gitlab_hook = GitlabHook(self.gitlab_conn_id)
        gitlab = gitlab_hook.get_conn()
        log.info("hook initialized for gitlab trigger. start tracking for {}".format(self.projects))
        changed_repos = []

        while True:
            self.runs += 1
            log.info("trying gitlab trigger for {} times out of {}".format(self.runs, self.check_runs))
            for project_id, branch in self.projects.items():
                log.info("Checking branch {} of project {} commits since {}.".format(
                    branch, project_id, self.changes_since))
                commits_result = await gitlab.async_get_commits(project_id=project_id,
                                                                branch_name=branch,
                                                                since=self.changes_since)

                if commits_result.success and len(commits_result.commits) > 0:
                    log.info("Changes detected for project {}".format(project_id))
                    changed_repos.append(project_id)

            log.info("changed repositories: {}".format(len(changed_repos)))
            log.info("+---------------------------------")
            if len(changed_repos) > 0 or self.runs >= self.check_runs:
                yield TriggerEvent(changed_repos)

            await asyncio.sleep(self.check_interval)

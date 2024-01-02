import json
from typing import Dict, Any
from urllib import parse as urlparser

import aiohttp
import requests

from utils.gitlab_result import CommitResult


class Gitlab:
    """A class that provides an async/sync interface to the Gitlab API.

    This class uses the requests and aiohttp libraries to make HTTP requests to the Gitlab API and returns the response data as Python objects.

    :param url: The base URL of the Gitlab server.
    :type url: str
    :param private_token: The personal access token for authentication. Defaults to None.
    :type private_token: str
    :param connection_timeout: The timeout for the HTTP connections in seconds. Defaults to 10.
    :type connection_timeout: int
    """
    # Constants for status codes
    STATUS_OK = 200
    STATUS_ERROR = 600

    # Constant for the API version
    API_VERSION = 'v4'
    def __init__(self,
                 url: str,
                 private_token: str = None,
                 connection_timeout: int = 10) -> None:
        self.url = url
        self.private_token = private_token
        self.connection_timeout = connection_timeout
        self.api_url = urlparser.urljoin(url, f'/api/{Gitlab.API_VERSION}/')

    async def async_get_commits(self, project_id: int, branch_name: str = 'master', since: str = None) -> CommitResult:
        """Gets the list of commits for a given project and branch asyncronously.

        This method sends a GET request to the /projects/{project_id}/repository/commits endpoint of the Gitlab API and returns the response data as a list of dictionaries.

        :param project_id: The ID of the project to query.
        :type project_id: int
        :param branch_name: The name of the branch to query. Defaults to 'master'.
        :type branch_name: str
        :param since: The date and time to start looking for commits, in ISO 8601 format. Defaults to None.
        :type since: str

        :return: A dictionary with the following keys:
         • success: A boolean indicating whether the request was successful or not.
         • message: A string containing an error message if the request failed, or None if it succeeded.
         • status: An integer representing the HTTP status code of the response. Status 600 means exception
         • commits: A list of dictionaries representing the commits, or an empty list if no commits are found.
        :rtype: CommitResult
        """
        url = self.__get_commit_url(project_id, branch_name, since)

        session_timeout = aiohttp.ClientTimeout(total=self.connection_timeout)
        async with aiohttp.ClientSession(timeout=session_timeout) as session:
            try:
                async with session.get(url) as resp:
                    text = await resp.text()
                    return CommitResult.result_from_dict(Gitlab.__process_commit_response(resp.status, text))
            except Exception as e:
                return CommitResult.result_from_dict(Gitlab.get_exception_result(e))

    def sync_get_commits(self, project_id: int, branch_name: str = 'master', since: str = None) -> CommitResult:
        """Gets the list of commits for a given project and branch syncronously.

        This method sends a GET request to the /projects/{project_id}/repository/commits endpoint of the Gitlab API and returns the response data as a list of dictionaries.

        :param project_id: The ID of the project to query.
        :type project_id: int
        :param branch_name: The name of the branch to query. Defaults to 'master'.
        :type branch_name: str
        :param since: The date and time to start looking for commits, in ISO 8601 format. Defaults to None.
        :type since: str

        :return: A dictionary with the following keys:
         • success: A boolean indicating whether the request was successful or not.
         • message: A string containing an error message if the request failed, or None if it succeeded.
         • status: An integer representing the HTTP status code of the response. Status 600 means exception
         • commits: A list of dictionaries representing the commits, or an empty list if no commits are found.
        :rtype: CommitResult
        """
        url = self.__get_commit_url(project_id, branch_name, since)
        try:
            resp = requests.get(url, timeout=self.connection_timeout)
            text = resp.content.decode('utf')
            return CommitResult.result_from_dict(Gitlab.__process_commit_response(resp.status_code, text))
        except Exception as e:
            return CommitResult.result_from_dict(Gitlab.get_exception_result(e))


    def __get_commit_url(self, project_id, branch_name, since):
        url = self.api_url
        url += f"{'' if url.endswith('/') else '/'}projects/{project_id}/repository/commits?ref_name={branch_name}"
        if since:
            url += f"&since={since}"

        if self.private_token:
            url += f'&private_token={self.private_token}'
        return url

    @staticmethod
    def get_exception_result(exception: Exception) -> Dict[str, Any]:
        result = Gitlab.__get_default_result()
        Gitlab.__set_result(result,
                            success=False,
                            status=600,
                            message=str(exception))
        return result

    @staticmethod
    def __process_commit_response(status: int, text: str) -> dict:
        result = Gitlab.__get_default_result()
        if status == Gitlab.STATUS_OK:
            text_json = json.loads(text)
            if type(text_json) is list:
                Gitlab.__set_result(result, commits=text_json)
            else:
                Gitlab.__set_result(result,
                                    success=False,
                                    message="result is not valid. result: {}".format(text_json),
                                    commits=[])
        else:
            Gitlab.__set_result(result,
                                status=Gitlab.STATUS_ERROR,
                                message="Gitlab response is not succeed")
        return result

    @staticmethod
    def __get_default_result(**kwargs):
        message : str | None = None
        result = {'success': True, 'message': message, 'status': 200}
        result.update(kwargs)
        return result

    @staticmethod
    def __set_result(result: dict, status: int = 200, success: bool = True, message: str = None, **kwargs) -> None:
        result['status'] = status
        result['message'] = message
        result['success'] = success and status == 200

        result.update(kwargs)



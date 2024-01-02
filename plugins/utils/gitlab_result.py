from typing import List, Any

class GitlabResult:
    __status: int
    __message: str
    __success: bool

    def __init__(self, status: int, message: str, success: bool) -> None:
        self.__status = status
        self.__message = message
        self.__success = success

    @property
    def status(self):
        return self.__status

    @property
    def message(self):
        return self.__message

    @property
    def success(self):
        return self.__success

class CommitResult(GitlabResult):
    __commits: List[Any]
    def __init__(self, status: int = 200, message: str = "", success: bool = True, commits: List[Any] = []):
        super().__init__(status, message, success)
        self.__commits = commits

    @property
    def commits(self) -> List[Any]:
        if not self.__commits:
            return []

        return self.__commits

    @staticmethod
    def result_from_dict(result: dict):
        # result must contain all fields
        return CommitResult(result['status'], result['message'], result['success'], result['commits'])

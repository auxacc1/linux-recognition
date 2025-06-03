class LinuxRecognitionError(Exception):

    def __init__(self, message: str = 'Linux linux_recognition error') -> None:
        super().__init__(message)


class ResponseError(LinuxRecognitionError):

    def __init__(self, message: str = 'Response error') -> None:
        super().__init__(message)


class DatabaseError(LinuxRecognitionError):

    def __init__(self, message: str = 'Database error') -> None:
        super().__init__(message)


class SQLTemplateError(LinuxRecognitionError):

    def __init__(self, message: str = 'SQL template error') -> None:
        super().__init__(message)


class LLMError(LinuxRecognitionError):

    def __init__(self, message: str = 'LLM error') -> None:
        super().__init__(message)


class DataDependencyError(LinuxRecognitionError):

    def __init__(self, message: str = 'Data dependency error') -> None:
        super().__init__(message)


class ContextPreparationError(LinuxRecognitionError):
    def __init__(self, message: str = 'Context preparation error') -> None:
        super().__init__(message)


class ProjectNotInitializedError(LinuxRecognitionError):
    def __init__(self, message: str = 'Project has not yet been initialized') -> None:
        super().__init__(message)

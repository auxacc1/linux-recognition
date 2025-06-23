from logging import getLogger
from os import getenv

from aiohttp import ClientSession, ClientError

from linux_recognition.log_management import get_error_details


logger = getLogger(__name__)


class SessionManager:

    def __init__(self) -> None:
        self._sessions = {}
        self._init_sessions()

    def get_session(self, session_name: str = 'common') -> ClientSession:
        sessions = self._sessions
        if session_name not in sessions or sessions[session_name].closed:
            self._init_session(session_name)
        return self._sessions[session_name]

    async def close_sessions(self) -> None:
        sessions = self._sessions
        for name in sessions:
            session: ClientSession = sessions[name]
            try:
                if not session.closed:
                    await session.close()
            except ClientError as e:
                message = f'An error occurred while trying to close aiohttp session'
                extra = get_error_details(e)
                extra['session_name'] = name
                logger.error(message, extra=extra)

    def _init_sessions(self) -> None:
        session_names = ['common', 'github', 'gitlab']
        for name in session_names:
            self._init_session(name)

    def _init_session(self, session_name: str) -> None:
        if session_name not in self._sessions or self._sessions[session_name].closed:
            self._sessions[session_name] = ClientSession()


def get_headers(session_name: str = 'common') -> dict[str, str]:
    user_agents_list = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/136.0.0.0 Safari/537.36',
    ]
    default_ua = {'User-Agent': user_agents_list[0]}
    instance_to_headers = {
        'common': default_ua,
        'github': {
            'User-Agent': getenv('LINUX_RECOGNITION__GITHUB_USER'),
            'Authorization': f'token {getenv('LINUX_RECOGNITION__GITHUB_TOKEN')}',
            'Accept': 'application/vnd.github.v3+json'
        },
        'gitlab': default_ua
    }
    return instance_to_headers[session_name]

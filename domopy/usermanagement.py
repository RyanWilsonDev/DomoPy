
"""

User APIs
---------
Create
    Create a User
Get
    Get a User
List
    List Users
Update
    Update a User
Delete
    Delete a User
"""

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib.oauth2_session import OAuth2Session

class DomoUsers:
    """Facade for Domo Users API."""

    _client = None
    _domo = None
    _token = None

    def __init__(self, clientId, clientSecret):
        """Configure Backend Client."""
        self._set_client(client_id=clientId)
        self._get_token(client_id=clientId, client_secret=clientSecret)

    def _set_client(self, client_id):
        """Create backend client for Domo API connection."""
        self._client = BackendApplicationClient(client_id=client_id)
        self._domo = OAuth2Session(client=self._client)

    def _get_token(self, client_id, client_secret):
        """Authenticate with Domo API and retrieve Auth Token."""
        token_url = 'https://api.domo.com/oauth/token?grant_type=client_credentials&scope=users'
        self._token = self._domo.fetch_token(
            token_url=token_url, client_id=client_id, client_secret=client_secret)

    def create_user(self):
        """Create new Domo User."""
        pass

    def list_users(self):
        """List Domo Users in Domo instance."""
        pass

    def get_user(self, user_id):
        """Get a Domo User by Id."""
        pass

    def update_user(self, user_id):
        """Update a Domo User."""
        pass

    def delete_user(self, user_id):
        """Delete a Domo User by Id."""
        pass

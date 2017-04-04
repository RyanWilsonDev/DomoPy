"""

Group APIs
----------
Create
    Create a new Group
Add
    Add a user to a group
Get
    Get info about a specified group
List
    Get a list of all groups
    Get a list of all users in a group
Update
    Update info for an existing group
Delete
    Delete a group
Remove
    Remove a user from a group
"""

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib.oauth2_session import OAuth2Session


class DomoGroups:
    """Facade for Domo Group API."""

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

    def create_group(self):
        """Create new Domo User Group."""
        pass

    def add_user(self, user_id, group_id):
        """Add a user to a given User Group."""
        pass

    def remove_user(self, user_id, group_id):
        """Remove a specified user from a given User Group."""
        pass

    def list_groups(self):
        """List Domo User Groups in Domo instance."""
        pass

    def list_group_users(self, group_id):
        """Get a Domo Users in a group."""
        pass

    def get_group(self, group_id):
        """Get info for a user group."""
        pass

    def update_group(self, group_id):
        """Update a Domo User Group."""
        pass

    def delete_user(self, group_id):
        """Delete a Domo User Group by Id."""
        pass

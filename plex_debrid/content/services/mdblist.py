#import modules
from base import *
#import parent modules
from content import classes
from ui.ui_print import *

name = 'MDBList'
api_key = ""
base_url = "https://mdblist.com/api"
lists = []  # List of MDBList list IDs to monitor
session = requests.Session()

def setup(cls, new=False):
    from content.services import setup
    setup(cls, new)


def _make_entry(item):
    """Convert an MDBList API item to a watchlist entry."""
    entry = SimpleNamespace(
        title=item.get('title', 'Unknown'),
        year=item.get('year'),
        type='show' if item.get('mediatype') == 'show' else 'movie',
        imdb_id=item.get('imdb_id'),
        tmdb_id=item.get('tmdb_id'),
        ratingKey=item.get('imdb_id'),
        watchlistedAt=item.get('rank', 0),
        user=[['mdblist', 'mdblist']],
    )
    entry.Guid = []
    entry.EID = []
    if item.get('imdb_id'):
        entry.Guid.append(SimpleNamespace(id=f"imdb://{item['imdb_id']}"))
        entry.EID.append(f"imdb://{item['imdb_id']}")
    if item.get('tmdb_id'):
        entry.Guid.append(SimpleNamespace(id=f"tmdb://{item['tmdb_id']}"))
        entry.EID.append(f"tmdb://{item['tmdb_id']}")
    return entry


class watchlist(classes.watchlist):
    autoremove = "none"

    def __init__(self):
        if not api_key:
            self.data = []
            return
        if len(lists) > 0:
            ui_print('[mdblist] getting all list items ...')
        self.data = []
        try:
            for list_id in lists:
                url = f"{base_url}/lists/{list_id}/items?apikey={api_key}"
                response = session.get(url, timeout=30)
                if response.status_code == 200:
                    try:
                        items = response.json()
                    except (ValueError, KeyError):
                        ui_print(f"[mdblist] invalid JSON response for list {list_id}")
                        continue
                    for item in items:
                        if not item.get('imdb_id'):
                            continue
                        entry = _make_entry(item)
                        # Deduplicate by IMDB ID
                        existing_keys = {getattr(d, 'ratingKey', None) for d in self.data}
                        if item['imdb_id'] not in existing_keys:
                            self.data.append(entry)
                else:
                    ui_print(f"[mdblist] error fetching list {list_id}: HTTP {response.status_code}")
            if len(lists) > 0:
                ui_print(f'[mdblist] found {len(self.data)} items across {len(lists)} list(s)')
        except Exception as e:
            ui_print(f"[mdblist] error: {str(e)}")

    def remove(self, item):
        # MDBList lists are read-only — just remove from local data
        if item in self.data:
            self.data.remove(item)

    def update(self):
        """Re-fetch lists and check for new items."""
        update = False
        existing_keys = {getattr(d, 'ratingKey', None) for d in self.data}
        try:
            for list_id in lists:
                url = f"{base_url}/lists/{list_id}/items?apikey={api_key}"
                response = session.get(url, timeout=30)
                if response.status_code == 200:
                    try:
                        items = response.json()
                    except (ValueError, KeyError):
                        continue
                    for item in items:
                        if not item.get('imdb_id'):
                            continue
                        if item['imdb_id'] not in existing_keys:
                            ui_print(f'[mdblist] new item found: "{item.get("title")}"')
                            entry = _make_entry(item)
                            self.data.append(entry)
                            existing_keys.add(item['imdb_id'])
                            update = True
        except Exception as e:
            ui_print(f"[mdblist] error during update: {str(e)}")
        return update


class library(classes.library):
    name = 'MDBList Library'

    def setup(cls, new=False):
        pass  # MDBList doesn't provide library services

    def __new__(cls):
        return []  # No local library concept

"""
# keep minimal required versions for each image
python nexus_cleaner.py

# keep last 20 images
python nexus_cleaner.py - c 20

# delete all older then 5 days
python nexus_cleaner.py -d 5

# delete all older then 5 days, also print info about all images
python nexus_cleaner.py -d 5 --full_into

# delete image with name `image1` older then 5 days
python nexus_cleaner.py -d 5 --names image1

"""

import argparse
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests

LOGGER = logging.getLogger()

MIN_COUNT_VERSIONS = 10


def parse_date(value: str):
    return datetime.fromisoformat(value)


@dataclass
class Asset:
    image_name: str
    version: str
    last_modified: datetime
    id: str

    def __str__(self):
        return f'{self.image_name}:{self.version} ({self.last_modified})'


@dataclass
class Params:
    count: int = MIN_COUNT_VERSIONS
    days: int = None
    names: list[str] = None
    test: bool = False
    full_info: bool = False


class NexusApi:
    config_name = 'nexus-cleaner.conf'
    url_path_assets = '/service/rest/v1/assets/'

    def __init__(self, params: Params):
        self.params = params
        self.session = None
        self.nexus_url = None
        self.login = None
        self.password = None
        self.statuses = {}
        self.read_config()

    def start(self):
        self.read_config()
        self.start_session()

    def read_config(self):
        file_path = Path(__file__)
        config_path = file_path.parent.parent / self.config_name
        if not config_path.exists():
            raise FileNotFoundError('Config file not found (nexus-cleaner.conf)')

        config_keys = {}
        with open(config_path) as f:
            for one in f:
                parts = one.strip().split('=', 1)
                if len(parts) == 2:
                    config_keys[parts[0]] = parts[1]

        self.nexus_url = config_keys.get('nexus_url')
        self.login = config_keys.get('login')
        self.password = config_keys.get('password')

        if not self.nexus_url:
            raise Warning(f'nexus_url not specified in {self.config_name}')
        if not self.login or not self.password:
            raise Warning(f'login or password not specified in {self.config_name}')

    def start_session(self):
        self.session = requests.Session()
        self.session.auth = (self.login, self.password)

    def make_get_request(self, url: str):
        if not url.startswith('http'):
            url = urljoin(self.nexus_url, url)
        res = self.session.get(url)
        if not res.ok:
            logging.warning('response not ok url=%s response=%s %s', url, res.status_code, res.text[:1000])
            return False, None
        try:
            return True, res.json()
        except Exception as e:
            logging.error('unable to parse response data url=%s error=%s response=%s', url, repr(e), res.text[:1000])
            return False, repr(e)

    def make_delete_request(self, url: str):
        if not url.startswith('http'):
            url = urljoin(self.nexus_url, url)
        res = self.session.delete(url)
        if not res.ok:
            logging.warning('response not ok url=%s response=%s %s', url, res.status_code, res.text[:1000])
            return False
        return True

    def get_repo_assets(self, repo_name: str):
        base_url = f'/service/rest/v1/search?repository={repo_name}'
        items = []
        cont_token = None
        while True:
            page_items, cont_token = self.fetch_items(base_url, cont_token)
            items.extend(page_items)
            if not cont_token:
                break

        grouped_by_name = defaultdict(list)
        for one in items:
            grouped_by_name[one['name']].append((one['version'], one['assets']))
        return grouped_by_name

    def fetch_items(self, base_url, cont_token=None):
        url = base_url
        if cont_token:
            url = f'{url}&continuationToken={cont_token}'
        ok, data = self.make_get_request(url)
        if not ok:
            raise Warning(f'Unable to fetch data from {url} ({data})')
        return data['items'], data['continuationToken']

    def sort_assets(self, grouped_by_name: dict):
        sorted_by_date = {}
        for name, name_items in grouped_by_name.items():
            if self.params.names and name not in self.params.names:
                continue
            name_items_info = []
            for version, assets in name_items:
                latest = sorted(assets, key=lambda x: parse_date(x['lastModified']))[-1]
                name_items_info.append(Asset(name, version, parse_date(latest['lastModified']), latest['id']))
            sorted_by_date[name] = sorted(name_items_info, key=lambda x: x.last_modified)
        return sorted_by_date


class NexusDockerCleaner(NexusApi):
    def do_delete(self):
        self.start_session()
        docker_repos = self.get_docker_repos()
        for repo_name in docker_repos:
            sorted_assets = self.sort_assets(self.get_repo_assets(repo_name))
            assets_for_del = self.prepare_assets_list(sorted_assets)
            for assets in assets_for_del.values():
                self.delete_old(assets)

    def get_docker_repos(self):
        ok, data = self.make_get_request('/service/rest/v1/repositories/')
        if not ok:
            raise Warning(f'get_docker_repos not ok {data}')
        return [o['name'] for o in data if o['format'] == 'docker' and o['type'] == 'hosted']

    def prepare_assets_list(self, grouped_and_sorted_items):
        result = {}
        compare_date = None
        if self.params.days:
            compare_date = datetime.now(timezone.utc) - timedelta(days=self.params.days)

        for name, name_versions in grouped_and_sorted_items.items():
            assets_to_del = name_versions[:-self.params.count]
            if compare_date is not None:
                assets_to_del = [one for one in assets_to_del if one.last_modified < compare_date]
            if assets_to_del:
                result[name] = assets_to_del
            if self.params.full_info:
                self.print_full_log(name, name_versions, assets_to_del)
        return result

    def delete_old(self, assets_list: list[Asset]):
        print()
        for one in assets_list:
            print('deleting', one, end=' .. ')
            if not self.params.test:
                res = self.make_delete_request(f'/service/rest/v1/assets/{one.id}')
            else:
                res = 'TEST DONE'
            print(res)
            LOGGER.info('deleted %s status=%s', one, res)

    @classmethod
    def print_full_log(cls, name, all_assets, assets_to_del):
        state_values = {True: '\u2714', False: '\u2715'}
        print()
        print(name)
        left_ids = {one.id for one in assets_to_del}
        for one in all_assets:
            print(f'{one} [keep: {state_values[one.id not in left_ids]}]')


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true', help='Test run')
    parser.add_argument('-d', '--days', type=int, required=False, help='Delete all older than specified days')
    parser.add_argument('-c', '--count', type=int, required=False, default=30, help='Number of versions to keep')
    parser.add_argument('-n', '--names', nargs='+', type=str, required=False, help='List of images names to delete')
    parser.add_argument('--full_info', action='store_true', help='Print all images versions')
    return parser


if __name__ == '__main__':
    args = create_parser().parse_args()
    if args.count < MIN_COUNT_VERSIONS:
        raise ValueError(f'count must be equals or greater than {MIN_COUNT_VERSIONS}')
    NexusDockerCleaner(Params(**args.__dict__)).do_delete()

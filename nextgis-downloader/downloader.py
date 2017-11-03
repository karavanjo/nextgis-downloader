__author__ = "Dmitry Barishnikov (dmitry.baryshnikov@nextgis.ru), Dmitry Kolesov (kolesov.dm@gmail.com)"
__copyright__ = "Copyright (C) NextGIS"
__license__ = "GPL v.2"

import os
import requests
import json
import time
import tempfile
import shutil
import re

from bs4 import BeautifulSoup

from utils import check_archive_fast, silent_remove
import credentials as creds

URL = 'https://earthexplorer.usgs.gov'
AUTH_URL = 'https://ers.cr.usgs.gov/login/'

PRODUCT = 'Landsat 8 OLI/TIRS C1 Level-1'
PRODUCTS = {
    'Landsat 8 OLI/TIRS C1 Level-1': {
        'id': 12864,
        'field_identifier_ids': ['text_20520_1', 'text_20520_2', 'text_20520_3', 'text_20520_4'],
        'select_ids': ['select_20522_5', 'select_20515_5', 'select_20510_4', 'select_20517_4',
                       'select_20518_4', 'select_20513_3', 'select_20519_3']
    }
}

FORMAT = 'LandsatLook Quality Image'
FORMATS = {
    'LandsatLook Quality Image': {
        'extension': '.jpg'
    },
    'Level-1 GeoTIFF Data Product': {
        'extension': '.tar.gz'
    }
}

MAX_SCENE_COUNT = 25000


def get_session_id(s, login, password):
    response = s.get(AUTH_URL)
    match = re.search(r'value="(.*?)" id="csrf_token"', response.content)
    csrf_token = match.group(1)

    payload = {'username': login, 'password': password, 'csrf_token': csrf_token}
    response = s.post(AUTH_URL, data=payload, allow_redirects=False)

    if response.status_code != 302:
        raise RuntimeError('Authentication Failed')


def set_empty_filter(session):
    payload = {
        'tab': 1,
        'destination': 2,
        'coordinates': [],
        'format': 'dms',
        'dStart': '',
        'dEnd': '',
        'searchType': 'Std',
        'includeUnknownCC': 1,
        'num': str(MAX_SCENE_COUNT),
        'months': ['', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11'],
        'pType': 'polygon'
    }

    _ = session.post(URL + '/tabs/save', data={'data': json.dumps(payload)})


def set_dataset(session, dataset_id):
    payload = {
        'tab': 2,
        'destination': 3,
        'cList': [dataset_id],
        'selected': dataset_id
    }

    _ = session.post(URL + '/tabs/save', data={'data': json.dumps(payload)})


def set_dataset_additional_criteria(session, dataset_id, identifiers):
    if not identifiers:
        identifiers = []

    field_identifier_ids = PRODUCTS[PRODUCT]['field_identifier_ids']

    if len(identifiers) > len(field_identifier_ids):
        raise Exception('Count of identifiers is to many. Max count of identifiers is {0}'
                        .format(len(field_identifier_ids)))

    filter_by_identifiers = dict()
    for i, identifier in enumerate(identifiers):
        filter_by_identifiers[field_identifier_ids[i]] = identifier

    filter_by_selects = {select_id: [''] for select_id in PRODUCTS[PRODUCT]['select_ids']}

    filters = dict(filter_by_identifiers.items() + filter_by_selects.items())

    payload = {
        'tab': 3,
        'destination': 4,
        'criteria': {
            dataset_id: filters
        },
        'selected': dataset_id}

    params = dict()
    params["data"] = json.dumps(payload)

    _ = session.post(URL + '/tabs/save', data=params)


def fill_metadata(session, scene):
    req = session.get(scene['metadata'])

    soup = BeautifulSoup(req.text, 'html.parser')
    for tr in soup.find_all('tr'):
        if tr.td is not None:
            scene[tr.td.a.string] = tr.td.next_sibling.next_sibling.string


def fill_download_options(session, scene):
    product_id = str(PRODUCTS[PRODUCT]['id'])
    headers = {
        'X-Requested-With': 'XMLHttpRequest'
    }
    req = session.get(URL + '/download/options/' + product_id + '/' + scene['id'], headers=headers)

    soup = BeautifulSoup(req.text, 'html.parser')
    for input in soup.find_all('input'):
        onclick = input['onclick']
        onclick = onclick.replace("'", "")
        onclick = onclick.replace("window.location=", "")
        if 'disabled' in input.attrs:
            print 'Skip download URL ' + onclick
        else:
            scene[unicode.strip(input.findNext('div').text)] = onclick


def download_scene(scene, login, password, result_dir, tmp_parent_path):
    """
    Download Landsat Scene. Return result filename or None if the scene can't be downloaded.

    :param scene:   Scene id
    :param login:   login
    :param password:    password
    :param result_dir:  directory for store the scene archive
    :return:    path to the archive or None if an error occurs
    """
    # Skip download if file exists
    filename = os.path.join(result_dir,
                            '{name}{extension}'.format(name=scene['id'], extension=FORMATS[FORMAT]['extension']))
    if os.path.isfile(filename):
        return filename

    data_format_keys = [key for key in scene.keys() if FORMAT in key]

    if data_format_keys:
        data_format_key = data_format_keys[0]
    else:
        print 'Format "{format}" is unavailable for scene "{scene_id}"'.format(format=FORMAT, scene_id=scene['id'])
        return None

    if scene[data_format_key]:
        download_url = scene[data_format_key]
        tmp_scene_file = tempfile.mktemp(dir=tmp_parent_path) + FORMATS[FORMAT]['extension']
        try:
            _download_file(login, password, download_url, tmp_scene_file)
        except Exception:
            print 'ERROR: Failed download "{format}" for scene "{scene_id}"'.format(format=FORMAT, scene_id=scene['id'])
            return None
        finally:
            if os.path.isfile(tmp_scene_file):
                shutil.move(tmp_scene_file, filename)
            print 'File "{file_name}" is downloaded'.format(file_name=filename)
    else:
        print 'ERROR: No url for "{format}" for scene "{scene_id}"'.format(format=FORMAT, scene_id=scene['id'])
        return None

    if FORMAT == 'Level-1 GeoTIFF Data Product':
        if check_archive_fast(filename):
            scene['downloaded'] = True
            print 'File "{file_name}" is checked successfully'.format(file_name=filename)
            return filename
        else:
            print 'Downloaded file "{file_name}" is broken. The file will be removed.'.format(file_name=filename)
            silent_remove(filename)
            return None


def get_scenes(login, password, identifiers):
    product_id = str(PRODUCTS[PRODUCT]['id'])

    session = requests.session()
    get_session_id(session, login, password)
    set_empty_filter(session)
    set_dataset(session, product_id)
    set_dataset_additional_criteria(session, product_id, identifiers)

    req = session.get('{url}/result/count?collection_id={product_id}&_={time}'
                      .format(url=URL, product_id=product_id, time=str(int(time.time() * 1000))))
    dictionary = req.json()

    scenes_count = int(dictionary.get('collectionCount'))
    print 'Received ' + dictionary.get('collectionCount') + ' scenes'

    if scenes_count == 0:
        return
    elif scenes_count > MAX_SCENE_COUNT:
        raise RuntimeError('Too mach scenes. Modify search criteria')

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': URL,
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache'
    }

    req = session.post(URL + '/result/index', data='collectionId=' + product_id, headers=headers)

    soup = BeautifulSoup(req.text, 'html.parser')

    scene_list = []

    for imgtag in soup.find_all('img'):
        id = imgtag['class']
        if id is not None:
            scene = dict()
            scene['id'] = id[0]
            scene['preview'] = imgtag['src'].replace('/browse/thumbnails/', '/browse/')
            scene['metadata'] = URL + '/form/metadatalookup/?collection_id=' + product_id + '&entity_id=' + scene['id']
            scene_list.append(scene)

    for scene in scene_list:
        fill_metadata(session, scene)
        fill_download_options(session, scene)

    return scene_list


def _download_file(login, password, url, filename):
    session = requests.session()
    get_session_id(session, login, password)
    r = session.get(url, stream=True)
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


if __name__ == "__main__":
    login = creds.login
    password = creds.password
    scenes = get_scenes(login=login, password=password, identifiers=['LC08_L1GT_156119_20170207_20170216_01_T2',
                                                                     'LC08_L1GT_156120_20170207_20170216_01_T2',
                                                                     'LC08_L1TP_003056_20170207_20170216_01_T1'])

    for s in scenes:
        print s['id']
        download_scene(s, login, password, '/tmp/', '/tmp')

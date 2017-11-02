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

PRODUCT_ID = '4923'
# LandSat-4,5 - 3119
# LandSat-7 (1999 - 2003) - 3372
# Landsat-7 (2003 - present) - 3373
# Landsat-8 - 4923

max_scene_count = 25000


def get_session_id(s, login, password):
    response = s.get(AUTH_URL)
    match = re.search(r'value="(.*?)" id="csrf_token"', response.content)
    csrf_token = match.group(1)

    payload = {'username': login, 'password': password, 'csrf_token': csrf_token}
    response = s.post(AUTH_URL, data=payload, allow_redirects=False)

    if response.status_code != 302:
        raise RuntimeError('Authentication Failed')


def set_filter(s, coordinates, date_from, date_to, month_list):
    payload = {
        'tab': 1,
        'destination': 2,
        'coordinates': coordinates,
        'format': 'dd',
        'dStart': date_from,
        'dEnd': date_to,
        'searchType': 'Std',
        'num': str(max_scene_count),
        'months': month_list,
        'pType': 'polygon'
    }

    params = {}
    params["data"] = json.dumps(payload)

    _ = s.post(URL + '/tabs/save', data=params)


def set_dataset_no(s, no):
    payload = {
        'tab': 2,
        'destination': 3,
        'cList': [no],
        'selected': no
    }

    params = dict()
    params["data"] = json.dumps(payload)

    _ = s.post(URL + '/tabs/save', data=params)


def set_dataset(s, no):
    payload = {
        'tab': 3,
        'destination': 4,
        'criteria': {
            no: {
                'select_10041_4': [''],
                'select_10040_6': [''],
                'select_10039_4': [''],
                'select_10037_5': [''],
                'select_10035_3': [''],
                'select_16067_4': [''],
                'select_17735_5': ['']
            }
        },
        'selected': no
    }

    params = dict()
    params["data"] = json.dumps(payload)

    _ = s.post(URL + '/tabs/save', data=params)


def fill_metadata(s, scene):
    req = s.get(scene['metadata'])

    soup = BeautifulSoup(req.text, 'html.parser')
    for tr in soup.find_all('tr'):
        if tr.td is not None:
            scene[tr.td.a.string] = tr.td.next_sibling.next_sibling.string


def fill_download_options(s, scene):
    headers = {
        'X-Requested-With': 'XMLHttpRequest'
    }
    req = s.get(URL + '/download/options/' + PRODUCT_ID + '/' + scene['id'], headers=headers)

    soup = BeautifulSoup(req.text, 'html.parser')
    for input in soup.find_all('input'):
        onclick = input['onclick']
        onclick = onclick.replace("'", "")
        onclick = onclick.replace("window.location=", "")
        if 'disabled' in input.attrs:
            print 'Skip download URL ' + onclick
        else:
            scene[input.div.string.strip()] = onclick


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
    filename = os.path.join(result_dir, '%s.tar.gz' % (scene['id'],))
    if os.path.isfile(filename):
        return filename

    for key in scene.keys():

        if key.find('Level 1 GeoTIFF Data Product') != -1:

            download_url = scene[key]

            try:
                # Save scene into temp file, move it to the destination after the downloading
                # (we don't want broken/unfinished files with the scene_ID names in the pool directory,
                #  see check_pool procedure)

                tmp_scene_file = tempfile.mktemp(dir=tmp_parent_path) + '.tar.gz'
                _download_file(login, password, download_url, tmp_scene_file)
            finally:
                if os.path.isfile(tmp_scene_file):
                    shutil.move(tmp_scene_file, filename)
            break

    if check_archive_fast(filename):
        scene['downloaded'] = True
        return filename
    else:
        silent_remove(filename)
        return None


def _coord_list_to_coords(point_list):
    """Transform string of coordinates to list of coordinates. The
    list can be usable for earthexplorer requests.

    :param point_list: list of points
    """
    coordinates = []
    for i in range(len(point_list)):
        point = point_list[i]
        x, y = point
        coordinates.append({'c': i, 'a': y, 'o': x})

    return coordinates


def get_scenes(date_from, date_to, login, password, point_list, month_list):
    coordinates = _coord_list_to_coords(point_list)

    s = requests.session()
    get_session_id(s, login, password)
    # set_filter(s, coordinates, date_from, date_to, month_list)
    set_dataset_no(s, PRODUCT_ID)
    set_dataset(s, PRODUCT_ID)

    req = s.get(URL + '/result/count?collection_id=' + PRODUCT_ID + '&_=' + str(int(time.time() * 1000)))
    dictionary = req.json()

    scenes_count = int(dictionary.get('collectionCount'))
    print 'Received ' + dictionary.get('collectionCount') + ' scenes'

    if scenes_count == 0:
        return
    elif scenes_count > max_scene_count:
        raise RuntimeError('Too mach scenes. Modify search criteria')

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': URL,
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache'
    }

    req = s.post(URL + '/result/index', data='collectionId=' + PRODUCT_ID, headers=headers)

    soup = BeautifulSoup(req.text, 'html.parser')

    scene_list = []

    for imgtag in soup.find_all('img'):
        id = imgtag['class']
        if id is not None:
            scene = dict()
            scene['id'] = id[0]
            scene['preview'] = imgtag['src'].replace('/browse/thumbnails/', '/browse/')
            scene['metadata'] = URL + '/form/metadatalookup/?collection_id=' + PRODUCT_ID + '&entity_id=' + scene['id']
            scene_list.append(scene)

    for scene in scene_list:
        fill_metadata(s, scene)
        fill_download_options(s, scene)

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
    points = [
        (65.274704, 57.422701),
        (65.126716, 57.425387),
        (65.038587, 57.370736),
        (65.048564, 57.319596),
        (65.045239, 57.295346),
        (65.169948, 57.260291),
        (65.246437, 57.26209),
        (65.30796, 57.225203),
        (65.416042, 57.223402),
        (65.499182, 57.198189),
        (65.570682, 57.186476),
        (65.73031, 57.170253),
        (65.861671, 57.157631),
        (66.036265, 57.213499),
        (66.071183, 57.265686),
        (65.735299, 57.343829),
        (65.520798, 57.393144),
        (65.520798, 57.393144),
        (65.274704, 57.422701)
    ]

    login = creds.login
    password = creds.password

    scenes = get_scenes(date_from='08/23/2014',
                        date_to='09/01/2015',
                        point_list=points,
                        login=login, password=password,
                        month_list=['8'])

    for s in scenes:
        print s['id']
        download_scene(s, login, password, '/tmp/', '/tmp')

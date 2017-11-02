
__author__ = "Dmitry Kolesov (kolesov.dm@gmail.com)"
__copyright__ = "Copyright (C) NextGIS"
__license__ = "GPL v.2"

import os
import zipfile
import tarfile

import shapely
from shapely.geometry import Polygon
from shapely.geometry import mapping
from shapely.geometry import box


class CoordinateConverter():
    """
        Tools to convert coordinates to working coordinate system from original one.
        And to return to original coordinate system.

        Original CS: [-180, -90, 180, 90] where x=0 is 0 Meridian
        Working CS: [0, -90, 360, 90] where x=0 is 180 Meridian

        In working CS you can calculate corrent simplified geometry
        only for geometies in bbox (for original CS) and in bbox_work (for working CS).

        bbox is region that crosses 180 Meridian and does not intersect 0 one (in original CS)
        bbox_work is bbox convertation to working CS.

        Changing the bbox (and therefore bbox_work) is only possible provided that
        bbox will be intersected by 180 Meridian and
        will not be intersected by 0 one/
    """
    bbox = [[18, 40, 180, 82], [18, -180, -168, 82]]
    bbox_work = [18, 40, 192, 82]

    @classmethod
    def isAvailableFor(cls, shapely_multipolygon):
        bbox_polygon = box(*cls.bbox_work)

        for shapely_polygon in shapely_multipolygon.geoms:
            shapely_polygon = cls.toWorkProj(shapely_polygon)
            if not shapely_polygon.within(bbox_polygon):
                return False

        return True

    @classmethod
    def toWorkProj(cls, shapely_polygon):
        """
            Create shapely.geometry.Polygon with coordinates in working CS.

            :param shapely_polygon: shapely.geometry.Polygon in original CS.
        """
        geojson_def = mapping(shapely_polygon)
        new_geojson_def = geojson_def

        poligons = geojson_def["coordinates"]
        new_poligones = []
        for poligon in poligons:
            new_poligon = []
            for point in poligon:
                new_point = list(point)
                if new_point[0] < 0:
                    new_point[0] = 360 + new_point[0]
                new_poligon.append(new_point)
            new_poligones.append(new_poligon)
        new_geojson_def["coordinates"] = new_poligones
        polygon = shapely.geometry.asShape(new_geojson_def)
        return polygon

    @classmethod
    def toOrignProj(cls, shapely_polygon):
        """
            Create shapely.geometry.Polygon with coordinates in original CS.

            :param shapely_polygon: shapely.geometry.Polygon in working CS.
        """
        geojson_def = mapping(shapely_polygon)
        new_geojson_def = geojson_def

        poligons = geojson_def["coordinates"]
        new_poligones = []
        for poligon in poligons:
            new_poligon = []
            for point in poligon:
                new_point = list(point)
                if new_point[0] > 180:
                    new_point[0] = new_point[0] - 360
                new_poligon.append(new_point)
            new_poligones.append(new_poligon)
        new_geojson_def["coordinates"] = new_poligones
        return shapely.geometry.asShape(new_geojson_def)

    @classmethod
    def intersectionWork(cls, shapely_polygon):
        """
            Create shapely.geometry.Polygon that intersected with bbox_work.

            :param shapely_polygon: shapely.geometry.Polygon in working CS.
        """
        (xmin, ymin, xmax, ymax) = cls.bbox_work

        geojson_def = mapping(shapely_polygon)
        new_geojson_def = geojson_def

        poligons = geojson_def["coordinates"]
        new_poligones = []
        for poligon in poligons:
            new_poligon = []
            prev_point = ()
            for point in poligon:
                new_point = list(point)

                if new_point[0] < xmin:
                    new_point[0] = xmin
                elif new_point[0] > xmax:
                    new_point[0] = xmax
                if new_point[1] < ymin:
                    new_point[1] = ymin
                elif new_point[1] > ymax:
                    new_point[1] = ymax

                if prev_point != new_point:
                    new_poligon.append(new_point)

            new_poligones.append(new_poligon)
        new_geojson_def["coordinates"] = new_poligones
        return shapely.geometry.asShape(new_geojson_def)


def simplify_geom(wkt, max_points=30):
    """Simplify geometry. The result is polygon, it
    contains <= max_points vertices.

    :param max_points: Threshold for allowed count of points.
    """
    if max_points <= 8:
        raise ValueError("Simplification can't be done (desired number of points is too small).")

    data = shapely.wkt.loads(wkt)

    # If data consists of several polygons,
    # use convex hull to cover all area
    if len(data.geoms) == 1:
        simplified = data.geoms[0]
    else:
        simplified = data.convex_hull

    simplified = CoordinateConverter.toWorkProj(simplified)
    simplified = CoordinateConverter.intersectionWork(simplified)
    simplified_orign = simplified

    # Simplification uses extending buffers:
    #   create buffer, then simplify
    #   if the result has more points then accepted, repeat with bigger buffer.
    # This procedure should converge after a finite number of iterations
    multiplier = 1.0
    while True:
        exterior = simplified.exterior
        result = CoordinateConverter.intersectionWork(Polygon(exterior))

        n_points = len(result.exterior.coords)
        if (n_points <= max_points) and (result.contains(simplified_orign)):
            break

        mean_dist = exterior.length / len(exterior.coords)
        dist = mean_dist * multiplier
        multiplier += 0.1

        buf = exterior.buffer(dist)
        simplified = buf.simplify(dist)

    return CoordinateConverter.toOrignProj(result).wkt


def silent_remove(filename):
    if os.path.exists(filename):
        os.remove(filename)


def zip(filename_list, arch_name):
    zf = zipfile.ZipFile(arch_name, mode='w', compression=zipfile.ZIP_DEFLATED, allowZip64=True)
    for filename in filename_list:
        zf.write(filename, os.path.basename(filename))
    zf.close()


def check_archive_fast(datafile):
    # There isn't way to check tar arch without unpacking
    # http://stackoverflow.com/questions/1788236/how-to-determine-if-data-is-valid-tar-file-without-a-file
    # We'll perform fast check
    return tarfile.is_tarfile(datafile)


def unpack(data_file, extract_dir):
    try:
        a = tarfile.open(data_file)
        a.extractall(path=extract_dir)
        a.close()
    except Exception:
        return False

    return True


def filename_to_bandnumber(geofilename):
    """
    Extract from Landsat geotif file the number of the band
    """
    i = geofilename.rindex('_')
    j = geofilename.rindex('.')
    num = geofilename[i+2: j]

    try:
        num = int(num)
    except ValueError:  # it is QA raster
        num = None

    return num


def find_meta(dirname):
    """
    Find metadata file for the scene

    :param dirname:     directory name of unpacked Landsat scene
    :return:    path to the metadata file
    """
    file_list = [f for f in os.listdir(dirname)
                 if os.path.isfile(os.path.join(dirname, f))]
    file_list = [os.path.join(dirname, f) for f in file_list if f.endswith('_MTL.txt')]
    if len(file_list) != 1:
        raise ValueError('Unknown format of Landsat archive')

    return file_list[0]


def get_raster_list(dirname):
    file_list = [f for f in os.listdir(dirname)
                 if os.path.isfile(os.path.join(dirname, f))]
    file_list = [os.path.join(dirname, f) for f in file_list if f.endswith('.TIF')]

    return file_list
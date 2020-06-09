import fiona
import ogr,json
# QGIS save as is better 
# this class does not consider coordinates system
class Shp2Json:
    def __init__(self, shapefile):
        self.shapefile = shapefile

    def shp2json_fiona(self):
        vector = fiona.open(self.shapefile, 'r')
        geomjson_list = []
        for feature in vector:
            # create a shapely geometry
            # this is done for the convenience for the .bounds property only
            # feature['geoemtry'] is in Json format
            geojson = feature['geometry']
            geomjson_list.append(geojson)
        return geomjson_list

    def shp2json_ogr(self):
        dr = ogr.GetDriverByName("ESRI Shapefile")
        shp_ds = dr.Open(self.shapefile)
        layer = shp_ds.GetLayer(0)
        # shp_proj = layer.GetSpatialRef()
        # shp_proj4 = shp_proj.ExportToProj4()
        # extent = layer.GetExtent()  # minx, maxx, miny,  maxy
        geomjson_list = []
        feat_num = layer.GetFeatureCount()
        for i in range(feat_num):
            feat = layer.GetFeature(i)
            geom = feat.GetGeometryRef()
            geojson = json.loads(geom.ExportToJson())
            geomjson_list.append(geojson)
        return geomjson_list



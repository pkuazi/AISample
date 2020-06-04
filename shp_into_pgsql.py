'''
Created on Dec 30, 2019

@author: root
'''
import pgsql
import os
import shapely, fiona
from asn1crypto._ffi import null
import datetime, time
from osgeo import ogr,osr
import json
import gjsonc
from geotrans import GeomTrans
import uuid

# data_root = "/mnt/gscloud/LANDSAT"
shp_path = "/mnt/win/phd/samples/pd_1995"
shp_file = os.path.join(shp_path,'PD_1995_120035.shp')
# geojson=os.path.join(shp_path, 'pd_1995_120035.geojson')

pg_src = pgsql.Pgsql("10.0.85.20", "postgres", "", "mark")

# def parse_shp_to_geojson(shpfile):
#     return geojson

def encode_json(t):
#     f=open(geojson_file)
#     t = json.loads(f.read())
    crs_wkt = t['crs']['properties']['name']
    num_geom=len(t['features'])
    for i in range(num_geom):
        print(i)
        geom = str(t['features'][i]['geometry'])
        geom_wgs = GeomTrans(crs_wkt, 'EPSG:4326').transform_json(geom)
        geojs=gjsonc.trunc_geojson(json.loads(geom_wgs),4)
        jstr = gjsonc.encode_geojson(geojs)
        t['features'][i]['geometry']=jstr
#         ruku(t['features'][i])
    return t    

def gjson_geotrans_to_wgs84(geojson, inproj):
    geom = ogr.CreateGeometryFromJson(geojson)
                 
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(4326) 
                
    inSpatialRef = osr.SpatialReference()
    inSpatialRef.ImportFromWkt(inproj)
        
    transform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef) 
    trans_state = geom.Transform(transform)
    return trans_state, geom

def get_curtime():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
def gen_uuid():
    return str(uuid.uuid4()).replace("-", "")

def labels_into_pgsql(geom, labelid,proj):
    sampletype =0
    imageres=30
    imageid="LC81200342015133LGN00"
    taskid = 11
    tagid=labelid
    
    guid=gen_uuid()
    ctime = get_curtime()
    mtime = datetime.datetime.now()
    
#     insert_sql = """INSERT INTO public.samples(
#             img, gid, userid, label, geom, ctime, taskid, name, abstract, 
#             sampleid, data, year, id, projection, type, source)
#     VALUES (0, null, 0, %s, %s, %s,  %s, %s, null, %s, null, 2015, %s, %s, %s, 1);  """
      
    insert_sql = """INSERT INTO sample( guid, geom, labelid, tagid, taskid, imageid, imageres, sampletype,mtime, projection)
    VALUES (%s,%s, %s, %s, %s, %s, %s,%s,%s,%s);    """
    update_sql = """UPDATE sample SET  guid=%s, geom=%s, labelid=%s, tagid=%s, taskid=%s, imageid=%s, imageres=%s, sampletype=%s, mtime=%s, projection=%s, imagetime=%s"""
    
    sql = "select * from sample where taskid='%s' " % (taskid)
    datas = pg_src.getAll(sql)

    if len(datas) == 0:
        pg_src.update(insert_sql, (guid,geom, labelid, labelid,taskid, imageid,30,0,mtime,proj))
        print("insert ", labelid)
    else:
        pg_src.update(update_sql, (guid,geom, labelid, labelid,taskid, imageid,30,0,mtime,proj))
        print("update ", labelid)
                

if __name__ == '__main__':   
    with fiona.open(shp_file, 'r') as inp:
        projection = inp.crs_wkt
        
        for f in inp:
            geojson = json.dumps(f['geometry'])
            trans_state, geom = gjson_geotrans_to_wgs84(geojson, projection)
            
            if trans_state==0:
#             geom_wgs = GeomTrans(projection, 'EPSG:4326').transform_geom(geom)
                wkt = geom.ExportToWkt()
                
                type = f['geometry']['type']
    #             data = f['properties']['data']
                label = f['properties']['class_id']
    #             name = label
    #             type = f['properties']['type']
    #             source=1
                if label==4 or label==5:
                    labels_into_pgsql(wkt,label,'EPSG:4326')
            



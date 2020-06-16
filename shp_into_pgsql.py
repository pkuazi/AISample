'''
Created on Dec 30, 2019

@author: root
'''
import utils.pgsql as pgsql
import os
import shapely, fiona
from asn1crypto._ffi import null
import datetime, time
from osgeo import ogr, osr
import json
import utils.geojsons as gjsonc
from utils.geotrans import GeomTrans
import uuid

# data_root = "/mnt/gscloud/LANDSAT"
shp_path = "/mnt/win/phd/samples/pd_1995"
shp_file = os.path.join(shp_path, 'PD_1995_120035.shp')
# geojson=os.path.join(shp_path, 'pd_1995_120035.geojson')

# pg_src = pgsql.Pgsql("10.0.85.20", "postgres", "", "mark")
pg_src = pgsql.Pgsql("10.0.81.19", "9999","postgres", "", "gscloud_web")

# def parse_shp_to_geojson(shpfile):
#     return geojson


def encode_json(t):
#     f=open(geojson_file)
#     t = json.loads(f.read())
    crs_wkt = t['crs']['properties']['name']
    num_geom = len(t['features'])
    for i in range(num_geom):
        print(i)
        geom = str(t['features'][i]['geometry'])
        geom_wgs = GeomTrans(crs_wkt, 'EPSG:4326').transform_json(geom)
        geojs = gjsonc.trunc_geojson(json.loads(geom_wgs), 4)
        jstr = gjsonc.encode_geojson(geojs)
        t['features'][i]['geometry'] = jstr
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


def labels_into_pgsql(geom, labelid, proj):
    sampletype = 0
    imageres = 30
    imageid = "LC81200342015133LGN00"
    taskid = 11
    tagid = labelid
    
    guid = gen_uuid()
    ctime = get_curtime()
    mtime = datetime.datetime.now()
    
#     insert_sql = """INSERT INTO public.samples(
#             img, gid, userid, label, geom, ctime, taskid, name, abstract, 
#             sampleid, data, year, id, projection, type, source)
#     VALUES (0, null, 0, %s, %s, %s,  %s, %s, null, %s, null, 2015, %s, %s, %s, 1);  """
      
    insert_sql = """INSERT INTO sample( guid, geom, labelid, tagid, taskid, refimage, imageres, sampletype,ctime, projection)
    VALUES (%s,%s, %s, %s, %s, %s, %s,%s,%s,%s);    """
    update_sql = """UPDATE sample SET  guid=%s, geom=%s, labelid=%s, tagid=%s, taskid=%s, imageid=%s, imageres=%s, sampletype=%s, mtime=%s, projection=%s, imagetime=%s"""
    
    sql = "select * from sample where taskid='%s' " % (taskid)
    datas = pg_src.getAll(sql)

    if len(datas) == 0:
        pg_src.update(insert_sql, (guid, geom, labelid, labelid, taskid, imageid, 30, 1, ctime, proj))
        print("insert ", labelid)
    else:
        pg_src.update(update_sql, (guid, geom, labelid, labelid, taskid, imageid, 30, 1, ctime, proj))
        print("update ", labelid)


def tasktiles_shp_into_pgsql(task_title, tile_shp, imageids):
    #     create a new task
    ctime = get_curtime()
    tag = '1: Cultivated, 2: Forest, 3: Grassland, 4: Construction, 5: Water, 6: Unused&others'
    insert_sql = '''INSERT INTO public.mark_task(abstract, active, ctime, detail, ftype,  is_public,  title, stime, tag, state,image)
VALUES(%s, %s, %s, %s, %s, %s,  %s , %s, %s, %s,%s);'''  
    update_sql = '''UPDATE public.mark_task SET abstract=%s, active=%s,  detail=%s, ftype=%s,  is_public=%s,  stime=%s, tag=%s, state=%s, image=%s
WHERE title = %s;''' 
    
    sql = "select id from public.mark_task where title='%s' " % (task_title)
    datas = pg_src.getAll(sql)

    if len(datas) == 0:
        pg_src.update(insert_sql, ('', '1', ctime, '', '3', '1', task_title, ctime, tag, '1' , imageids))
        print("insert ", task_title)
        sql = "select id from public.mark_task where title='%s' " % (task_title)
        datas = pg_src.getAll(sql)
        taskid = datas[0][0]
    else:
        pg_src.update(update_sql, ('', '1', '', '3', '1', ctime, tag, '1', imageids, task_title))
        print("update ", task_title)
        taskid = datas[0][0]
     
     #    insert tiles as subtask
    with fiona.open(tile_shp, 'r') as inp:
        projection = inp.crs_wkt
        for f in inp:
            geojson = json.dumps(f['geometry'])
            trans_state, geom = gjson_geotrans_to_wgs84(geojson, projection)
            
            if trans_state == 0:
                wkt = geom.ExportToWkt()
                type = f['geometry']['type']
                row = f['properties']['row']
                col = f['properties']['col']                         
#                 guid=gen_uuid()
                
                row_s = '0' + str(row)           
                col_s = '0' + str(col) 
                guid = task_title+'_'+row_s[-2:] + '_' + col_s[-2:]
                ctime = get_curtime()
                
                insert_sql = '''INSERT INTO public.mark_subtask
(guid, taskid, ctime,  geojson )
VALUES(%s ,%s, %s, %s);
'''
                update_sql = '''UPDATE public.mark_subtask
SET guid=%s, taskid=%s, ctime=%s, geojson=%s;
'''
                
                sql = "select id from public.mark_subtask where guid like '%s' " % (guid)
                datas = pg_src.getAll(sql)
            
                if len(datas) == 0:
                    pg_src.update(insert_sql, (guid, taskid, ctime, wkt))
                    print("insert subtask tile of ", guid)
                else:
                    pg_src.update(update_sql, (guid, taskid, ctime, wkt))
                    print("insert subtask tile of ", guid)            
def get_taskid_by_tasktitle(task_title):
    task_search_sql = '''SELECT id FROM public.mark_task where title='%s';'''%(task_title)
    data = pg_src.getAll(task_search_sql)
    taskid = data[0][0]    
    return taskid    
def get_wkt_by_tasktitle(task_title):
    task_search_sql = '''SELECT geojson FROM public.mark_task where title='%s';'''%(task_title)
    data = pg_src.getAll(task_search_sql)
    region_wkt = data[0][0]    
    return region_wkt  
if __name__ == '__main__':   
    print('test')
    # bj_2001: LT51230322001323BJC00  LT51230332001323BJC00
#     subtask tiles into pgsql
    for region in region_dict.keys():
#         # region_tiles_shp = os.path.join(region_bbox_path,(region + '_subtiles.shp'))
#             # region is one of the region_dict.keys()
#         region_tif = region_dict[region]['region_tif']
#         region_file = os.path.join(region_tif_path, region_tif)
#          
#         # print('row,col: %s, %s'%(rnum,cnum))
#         images_key = region_dict[region]['images_key']
#         year_list = region_dict[region]['year']
#          
        for year in year_list:
            tile_shp = os.path.join(region_bbox_path,(region + '_'+str(year)+'_'+'tiles.shp'))
#             wgs_bbox_list, rnum, cnum, region_bbox = gen_tile_bbox(region_file,BLOCK_SIZE, OVERLAP_SIZE)
#             tile_bbox_to_shp(wgs_bbox_list, rnum, cnum, tile_shp)
            if not os.path.exists(tile_shp):
                print('the tiling shapefile does not exists')
                continue
#              
            imageids = get_imageids(images_key=images_key, year=year)
            task_title= region + '_'+str(year)
            tasktiles_shp_into_pgsql(task_title, tile_shp, imageids)
            
#     with fiona.open(shp_file, 'r') as inp:
#         projection = inp.crs_wkt
#         
#         for f in inp:
#             geojson = json.dumps(f['geometry'])
#             trans_state, geom = gjson_geotrans_to_wgs84(geojson, projection)
#             
#             if trans_state == 0:
# #             geom_wgs = GeomTrans(projection, 'EPSG:4326').transform_geom(geom)
#                 wkt = geom.ExportToWkt()
#                 
#                 type = f['geometry']['type']
#     #             data = f['properties']['data']
#                 label = f['properties']['class_id']
#     #             name = label
#     #             type = f['properties']['type']
#     #             source=1
#                 if label == 4 or label == 5:
#                     labels_into_pgsql(wkt, label, 'EPSG:4326')

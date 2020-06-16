import fiona
import utils.pgsql as pgsql
from shapely.geometry import mapping, Polygon

# pg_src = pgsql.Pgsql("10.0.81.35", "2345","postgres", "", "gscloud_metadata")
pg_src = pgsql.Pgsql("10.0.81.19", "9999","postgres", "", "gscloud_web")

def update_task_table(task_title):
    region_shp=''
    gtfile=''
    task_update_sql = '''UPDATE public.mark_task SET geojson=%s, gtfile=%s title='%s';'''
    pg_src.update(task_update_sql, (region_shp, gtfile, task_title))

def region_query_tiles(imageid, imagebbox, task_title):
#     query taskid
#     query subtask contained in imagebbox ('POLYGON ((116 40,1179 413,117 39,116 39,116 40))')
    task_search_sql = '''SELECT id FROM public.mark_task where title='%s';'''%(task_title)
    data = pg_src.getAll(task_search_sql)
    taskid = data[0][0]
    tile_update_imageid_sql = '''UPDATE public.mark_subtask SET imageid=%s where taskid='%s' and ST_Contains(st_geomfromtext(%s), geojson);'''
    pg_src.update(tile_update_imageid_sql, (imageid, taskid, imagebbox))

def query_tiles_by_tasktitle(task_title):
    task_search_sql = '''SELECT id FROM public.mark_task where title='%s';'''%(task_title)
    data = pg_src.getAll(task_search_sql)
    taskid = data[0][0]
    tiles_search_sql = '''SELECT guid, geojson, imageid, sid FROM public.mark_subtask where taskid='%s';'''%(taskid)
    print(tiles_search_sql)
    data1 = pg_src.getAll(tiles_search_sql)
#     print(data1)
    return data1

def image_query(product, geom, year, month):
    shp_file = '/mnt/win/data/AISample/region_bbox/bj_subbox.shp'
    with fiona.open(shp_file, 'r') as inp:
        projection = inp.crs_wkt
        prj = inp.crs
        prj_epsg_int = int(prj['init'][5:9])
        for f in inp:           
            geojson = json.dumps(f['geometry'])
            geom = ogr.CreateGeometryFromJson(geojson)
            wkt = geom.ExportToWkt()
            '''
              SELECT dataid, satellite, datatype, datadate, datadate_year, datadate_month, 
       datadate_day, cloudcover, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long, rb_lat, lb_long, 
       lb_lat, dataexists, layerexists, the_geom FROM public.metadata_landsat_oli_tirs WHERE ST_Contains(the_geom, st_geomfromtext('POLYGON ((116.155363384491 40.546137617483,116.246828353439 40.546137617483,116.246828353439 40.4775818941055,116.155363384491 40.4775818941055,116.155363384491 40.546137617483))',4326)) ORDER BY cloudcover ASC limit 10;

            '''
    region_query_sql = '''SELECT dataid, satellite, datatype, datadate, datadate_year, datadate_month, 
       datadate_day, cloudcover, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long, rb_lat, lb_long, 
       lb_lat, dataexists, layerexists, the_geom FROM public.%s WHERE ST_Contains(the_geom, st_geomfromtext('%s',%s)) ORDER BY cloudcover ASC limit 10;''' % ('metadata_landsat_oli_tirs', wkt, prj_epsg_int)
    print(region_query_sql)
    data = pg_src.getAll(region_query_sql)
    num = len(data)
    print(num)
    region_query_sql = '''SELECT dataid, satellite, datatype, path, "row", datadate, datadate_year, datadate_month, 
       datadate_day, cloudcover, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long, rb_lat, lb_long, 
       lb_lat, dataexists, layerexists, the_geom
  FROM public.%s WHERE ST_Contains(the_geom, %s) ORDER BY cloudcover ASC limit 10;''' % ('metadata_landsat_oli_tirs', geom)
    data = pg_src.getAll(region_query_sql)
    
    data_sql = '''SELECT id, dataid, name, "path", "row",  lt_long, lt_lat,  rb_long, rb_lat,the_geom FROM public.metadata_dem_gdem where rb_long>%s and lt_long<%s and rb_lat<%s and lt_lat>%s ORDER BY row DESC;''' % (min_long, max_long, max_lat, min_lat)
    dem_data = pg_src.getAll(data_sql)
    num = len(dem_data)
    
    # output bounding box into shp
    dataid_list = []
    
     # schema is a dictory
    schema = {'geometry': 'Polygon', 'properties': {'id': 'int', 'dataid': 'str', 'path':'int', 'row':'int'} }
    #  use fiona.open
    with fiona.open(dst_shp, mode='w', driver='ESRI Shapefile', schema=schema, crs='EPSG:4326', encoding='utf-8') as layer:
        for i in range(num):
            record = dem_data[i]
            bbox = dem_data[i][9]
            dataid = dem_data[i][1]
            if dataid.startswith('ASTGTM2'):
                dataid_list.append(dataid)
                minx, maxy, maxx, miny = dem_data[i][5], dem_data[i][6], dem_data[i][7], dem_data[i][8]
                poly = Polygon([[minx, maxy], [maxx, maxy], [maxx, miny], [minx, miny], [minx, maxy]])
                element = {'geometry':mapping(poly), 'properties': {'id': i, 'dataid': dataid, 'path':dem_data[i][3], 'row':dem_data[i][4]}}
                layer.write(element)     
    return  dataid_list 

def region_search_dem_toshp(min_lat, max_lat, min_long, max_long, dst_shp):
    data_sql = '''SELECT id, dataid, name, "path", "row",  lt_long, lt_lat,  rb_long, rb_lat,the_geom FROM public.metadata_dem_gdem where rb_long>%s and lt_long<%s and rb_lat<%s and lt_lat>%s ORDER BY row DESC;'''%(min_long,max_long,max_lat,min_lat)
    dem_data = pg_src.getAll(data_sql)
    num = len(dem_data)
    
    #output bounding box into shp
    dataid_list=[]
    
     # schema is a dictory
    schema={'geometry': 'Polygon', 'properties': {'id': 'int', 'dataid': 'str', 'path':'int','row':'int'} }
    #  use fiona.open
    with fiona.open(dst_shp, mode='w', driver='ESRI Shapefile', schema=schema, crs='EPSG:4326', encoding='utf-8') as layer:
        for i in range(num):
            record = dem_data[i]
            bbox = dem_data[i][9]
            dataid = dem_data[i][1]
            if dataid.startswith('ASTGTM2'):
                dataid_list.append(dataid)
                minx,maxy,maxx,miny = dem_data[i][5],dem_data[i][6],dem_data[i][7],dem_data[i][8]
                poly=Polygon([[minx,maxy],[maxx,maxy],[maxx,miny],[minx,miny],[minx,maxy]])
                element = {'geometry':mapping(poly), 'properties': {'id': i, 'dataid': dataid,'path':dem_data[i][3],'row':dem_data[i][4]}}
                layer.write(element)     
    return  dataid_list  

def region_search_dem(min_lat, max_lat, min_long, max_long):
    pg_src = pgsql.Pgsql("10.0.81.35", "2345","postgres", "", "gscloud_metadata")
    data_sql = '''SELECT id, dataid, name, "path", "row",  lt_long, lt_lat,  rb_long, rb_lat,the_geom FROM public.metadata_dem_gdem where rb_long>%s and lt_long<%s and rb_lat<%s and lt_lat>%s ORDER BY row DESC;'''%(min_long,max_long,max_lat,min_lat)
    dem_data = pg_src.getAll(data_sql)
    num = len(dem_data)
    
    #output bounding box into shp
    dataid_list=[]

    for i in range(num):
        record = dem_data[i]
        bbox = dem_data[i][9]
        dataid = dem_data[i][1]
        if dataid.startswith('ASTGTM2'):
            dataid_list.append(dataid)  
    return  dataid_list  
def region_search_srtm(min_lat, max_lat, min_long, max_long, dst_shp):
    data_sql = '''SELECT id, dataid, "path", "row",  lt_long, lt_lat,  rb_long, rb_lat FROM public.metadata_dem_srtm where lt_long>%s and rb_long<%s and lt_lat<%s and rb_lat>%s ORDER BY row DESC;'''%(min_long,max_long,max_lat,min_lat)
    dem_data = pg_src.getAll(data_sql)
    num = len(dem_data)
    
    #output bounding box into shp
    dataid_list=[]
    
     # schema is a dictory
    schema={'geometry': 'Polygon', 'properties': {'id': 'int', 'dataid': 'str', 'path':'int','row':'int'} }
    #  use fiona.open
    with fiona.open(dst_shp, mode='w', driver='ESRI Shapefile', schema=schema, crs='EPSG:4326', encoding='utf-8') as layer:
        for i in range(num):
            record = dem_data[i]

            dataid = dem_data[i][1]
            if dataid.startswith('srtm'):
                dataid_list.append(dataid)
                minx,maxy,maxx,miny = dem_data[i][4],dem_data[i][5],dem_data[i][6],dem_data[i][7]
                poly=Polygon([[minx,maxy],[maxx,maxy],[maxx,miny],[minx,miny],[minx,maxy]])
                element = {'geometry':mapping(poly), 'properties': {'id': i, 'dataid': dataid,'path':dem_data[i][3],'row':dem_data[i][4]}}
                layer.write(element)     
    return  dataid_list  

def merge_all_dem(dataid_list, dstfile_path):
    num = len(dataid_list)
    srcfiles1 = ''
    for i in range(num):
        dataid = dataid_list[i]
        print(dataid)
        row = int(dataid[9:11])
        path = int(dataid[12:15])
        
        dem_path = '/mnt/gscloud/DEM/unzip0/GDTM/'
        data_path = os.path.join(dem_path,dataid_list[i])
        srcfiles1 = srcfiles1+os.path.join(data_path,str(dataid_list[i])+'_dem.tif')
        srcfiles1 = srcfiles1+' '
        if not os.path.exists(data_path):
            print(data_path)
            unzip_path=os.path.join(dem_path,dataid)
            mkdir_cmd = 'mkdir %s'%(unzip_path)
            os.system(mkdir_cmd)
            unzip_cmd = 'unzip %s -d %s'%(os.path.join("/mnt/gscloud/DEM/gdem30v2/gdem30v2",dataid+'.zip'),unzip_path)
            os.system(unzip_cmd)

    merge_cmd1 = 'gdalwarp %s %s'%(srcfiles1, dstfile_path)
    os.system(merge_cmd1) 

def merge_spec_dem(dataid_list, row_min, row_max, path_min,path_max, dstfile_path):
    #     dem_path='/mnt/gscloud/DEM/gdem30v2/gdem30v2'
    dem_path = '/mnt/gscloud/DEM/unzip0/GDTM/'
    dataname='/mnt/gscloud/DEM/unzip/GDTM/ASTGTM2_N34W087/ASTGTM2_N34W087_dem.tif'
    num = len(dataid_list)
    srcfiles1 = ''
    for i in range(num):
        dataid = dataid_list[i]
        print(dataid)
        row = int(dataid[9:11])
        path = int(dataid[12:15])
        if path>=path_min and path <=path_max:
            if row>=row_min and row <=row_max:
                data_path = os.path.join(dem_path,dataid_list[i])
                srcfiles1 = srcfiles1+os.path.join(data_path,str(dataid_list[i])+'_dem.tif')
                srcfiles1 = srcfiles1+' '
                if not os.path.exists(data_path):
                    print(data_path)
                    unzip_path=os.path.join(dem_path,dataid)
                    mkdir_cmd = 'mkdir %s'%(unzip_path)
                    os.system(mkdir_cmd)
                    unzip_cmd = 'unzip %s -d %s'%(os.path.join("/mnt/gscloud/DEM/gdem30v2/gdem30v2",dataid+'.zip'),unzip_path)
                    os.system(unzip_cmd)

    merge_cmd1 = 'gdalwarp %s %s'%(srcfiles1, dstfile_path)
    os.system(merge_cmd1) 
    
# def merge_spec_srtm(dataid_list, row_min, row_max, path_min,path_max, dstfile_path):
#     TODO
    #     dem_path='/mnt/gscloud/DEM/gdem30v2/gdem30v2'
#     dem_path = '/mnt/gscloud/DEM/unzip0/...'
#     dataname='/mnt/gscloud/DEM/unzip/.../ASTGTM2_N34W087/ASTGTM2_N34W087_dem.tif'
#     num = len(dataid_list)
#     srcfiles1 = ''
#     for i in range(num):
#         dataid = dataid_list[i]
#         print(dataid)
#         row = int(dataid[9:11])
#         path = int(dataid[12:15])
#         if path>=path_min and path <=path_max:
#             if row>=row_min and row <=row_max:
#                 data_path = os.path.join(dem_path,dataid_list[i])
#                 srcfiles1 = srcfiles1+os.path.join(data_path,str(dataid_list[i])+'_dem.tif')
#                 srcfiles1 = srcfiles1+' '
#                 if not os.path.exists(data_path):
#                     print(data_path)
#                     unzip_path=os.path.join(dem_path,dataid)
#                     mkdir_cmd = 'mkdir %s'%(unzip_path)
#                     os.system(mkdir_cmd)
#                     unzip_cmd = 'unzip %s -d %s'%(os.path.join("/mnt/gscloud/DEM/.../gdem30v2",dataid+'.zip'),unzip_path)
#                     os.system(unzip_cmd)
# 
#     merge_cmd1 = 'gdalwarp %s %s'%(srcfiles1, dstfile_path)
#     os.system(merge_cmd1) 

import os,gdal
def getbox_from_image(file):
    ds = gdal.Open(file)
    rows, cols=ds.RasterYSize, ds.RasterXSize
    geot=ds.GetGeoTransform()
    minx=geot[0]
    maxy=geot[3]
    maxx=minx+cols*geot[1]
    miny =maxy+geot[5]*rows
    return minx,maxy,maxx,miny

path=os.getcwd()

import pandas as pd
def bbox_pd_sort(path):
    all_files = [f for f in os.listdir(path)]
    
    file_list=[]
    minx_list=[]
    maxy_list=[]
    maxx_list=[]
    miny_list=[]
    for i in range(len(all_files)):
        if all_files[i].endswith('.tif'):
            bbox = getbox(os.path.join(path,all_files[i]))
            file_list.append(all_files[i])
            minx_list.append(bbox[0])
            maxy_list.append(bbox[1])
            maxx_list.append(bbox[2])
            miny_list.append(bbox[3])
    bb = {}
    bb["filename"]=file_list
    bb["minx"]=minx_list
    bb["maxy"]=maxy_list
    bb["maxx"]=maxx_list
    bb["miny"]=miny_list
    
    df=pd.DataFrame(bb)
    yd=df.sort_values(by='maxy',ascending=False)
    yd.to_csv('yd.csv')
    xs=df.sort_values(by='minx', ascending=True)
    xs.to_csv('xs.csv')

if __name__ == "__main__":
    
    tiles_search_sql = '''SELECT guid, geojson, imageid FROM public.mark_subtask where id=7275;'''
    print(tiles_search_sql)
    data1 = pg_src.getAll(tiles_search_sql)
    imageid = data1[0][2]
    if imageid is None:
        print('kkkk')
#     sh_minx=121.3015010490833561
#     sh_miny=30.6565199377234556 
#     sh_maxx=121.6328280392743011
#     sh_maxy=31.5169980147194799
#     sh_data = region_search_dem(sh_miny, sh_maxy, sh_minx, sh_maxx,'/tmp/sh.shp')
#     print('sh',len(sh_data))
#     merge_all_dem(sh_data,'/mnt/tmp/sh_dem.tif')
#     
#     bj_minx=116.1680000000000064
#     bj_miny=39.4778999999999769
#     bj_maxx=116.5760000000000218
#     bj_maxy=40.5480000000000587
#     bj_data = region_search_dem(bj_miny, bj_maxy, bj_minx, bj_maxx,'/tmp/bj.shp')
#     merge_all_dem(bj_data,'/mnt/tmp/bj_dem.tif')
#     
#     pd_minx=119.5250070000000733
#     pd_miny=36.4714850000000794 
#     pd_maxx=120.3205460000000642
#     pd_maxy=37.0462860000000660
#     pd_data = region_search_dem(pd_miny, pd_maxy, pd_minx, pd_maxx,'/tmp/pd.shp')
#     merge_all_dem(pd_data,'/mnt/tmp/pd_dem.tif')
#     
#     db_minx=107.2476430000000676
#     db_miny=36.8159350000000600
#     db_maxx=108.3707290000000683
#     db_maxy=37.8878510000000830
#     db_data = region_search_dem(db_miny, db_maxy, db_minx, db_maxx,'/tmp/db.shp')
#     merge_all_dem(db_data,'/mnt/tmp/db_dem.tif')
#     
#     ys_minx=118.1789894702706363
#     ys_miny=35.5889882607407557
#     ys_maxx=119.0702794462647347
#     ys_maxy=36.2181116042871736
#     ys_data = region_search_dem(ys_miny, ys_maxy, ys_minx, ys_maxx,'/tmp/ys.shp')
#     merge_all_dem(ys_data,'/mnt/tmp/ys_dem.tif')


import os
from osgeo import gdal, ogr,osr
import fiona
from utils.geotrans import GeomTrans
from shapely.geometry import mapping, Polygon
import pandas as pd
import utils.pgsql as pgsql
import json

sample_region = '/mnt/win/data/AISample/region_raster'
region_files = os.listdir(sample_region)
region_dict = {'bj':{'region':os.path.join(sample_region, 'bj.tif'), 'year':[2001, 2003, 2004]},
               'cd':{'region':os.path.join(sample_region, 'cd.tif'), 'year':[1990, 2000, 2010, 2015]},
               'liangji':{'region':os.path.join(sample_region, 'liangji.tif'), 'year':[2015]},
               'mws':{'region':os.path.join(sample_region, 'mws.tif'), 'year':[1978, 2000, 2015]},
               'PD':{'region':os.path.join(sample_region, 'PD.tif'), 'year':[1995, 2005, 2015]},
               'shanghai':{'region':os.path.join(sample_region, 'shanghai.tif'), 'year':[2006, 2009]},
               "sjz":{'region':os.path.join(sample_region, 'sjz.tif'), 'year':[2013]},
               'wuhan':{'region':os.path.join(sample_region, 'wuhan.tif'), 'year':[2015]},
               'xiaoshan':{'region':os.path.join(sample_region, 'xiaoshan.tif'), 'year':[1996,2001,2006, 2013]},
               'yishui':{'region':os.path.join(sample_region, 'yishui.tif'), 'year':[1995,2005,2015]},
               'zjk':{'region':os.path.join(sample_region, 'zjk.tif'), 'year':[1990, 2000, 2010, 2015]},
               }

BLOCK_SIZE = 256
OVERLAP_SIZE = 13

pg_src = pgsql.Pgsql("10.0.81.35", "2345","postgres", "", "gscloud_metadata")

def gjson_geotrans_to_wgs84(geojson, inproj):
    geom = ogr.CreateGeometryFromJson(geojson)
                 
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(4326) 
                
    inSpatialRef = osr.SpatialReference()
    inSpatialRef.ImportFromWkt(inproj)
        
    transform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef) 
    trans_state = geom.Transform(transform)
    return trans_state, geom

def image_query(product, geom, year, month):
    region_query_sql = '''SELECT dataid, satellite, datatype, path, "row", datadate, datadate_year, datadate_month, 
       datadate_day, cloudcover, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long, rb_lat, lb_long, 
       lb_lat, dataexists, layerexists, the_geom
  FROM public.%s WHERE ST_Contains(the_geom, %s) ORDER BY cloudcover ASC limit 10;'''%('metadata_landsat_oli_tirs', geom)
    data = pg_src.getAll(region_query_sql)
    
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
    
def region_search_dem(min_lat, max_lat, min_long, max_long, dst_shp):
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


def gen_subtask_bbox(rasterfile, tileid, dst_shp):
    print('the image is :', rasterfile)
    dataset = gdal.Open(rasterfile)
    if dataset is None:
        print("Failed to open file: " + rasterfile)
        sys.exit(1)
    band = dataset.GetRasterBand(1)
    xsize = dataset.RasterXSize
    ysize = dataset.RasterYSize
    proj = dataset.GetProjection()
    geotrans = dataset.GetGeoTransform()
    gt = list(geotrans)
    noDataValue = band.GetNoDataValue()
    
    rnum_tile = int((ysize - BLOCK_SIZE) / (BLOCK_SIZE - OVERLAP_SIZE)) + 1
    cnum_tile = int((xsize - BLOCK_SIZE) / (BLOCK_SIZE - OVERLAP_SIZE)) + 1
    print('the number of tile is :', rnum_tile * cnum_tile)
    
    subtask_list = []
    minx_list = []
    maxy_list = []
    maxx_list = []
    miny_list = []
    
     # schema is a dictory
    schema = {'geometry': 'Polygon', 'properties': {'id': 'int', 'dataid': 'str', 'row':'int', 'col':'int'} }
    with fiona.open(dst_shp, mode='w', driver='ESRI Shapefile', schema=schema, crs='EPSG:4326', encoding='utf-8') as layer:
        for i in range(rnum_tile + 1):
            print(i)
            for j in range(cnum_tile + 1):
                xoff = 0 + (BLOCK_SIZE - OVERLAP_SIZE) * j
                yoff = 0 + (BLOCK_SIZE - OVERLAP_SIZE) * i
                print("the row and column of tile is :", xoff, yoff)
    
                gt[0] = geotrans[0] + xoff * geotrans[1]
                gt[3] = geotrans[3] + yoff * geotrans[5]
                
                subtask_list.append(tileid + str(i) + '_' + str(j))
                
                minx = gt[0]
                maxy = gt[3]
                maxx = gt[0] + BLOCK_SIZE * geotrans[1]
                miny = gt[3] + BLOCK_SIZE * geotrans[5]
                
                #the last column                 
                if j == cnum_tile:
                    maxx = geotrans[0] + xsize * geotrans[1]
                    minx = maxx - BLOCK_SIZE * geotrans[1]
                #the last row
                if i == rnum_tile:
                    miny = geotrans[3] + ysize * geotrans[5]
                    maxy = miny - BLOCK_SIZE * geotrans[5]
                
                minx_wgs, maxy_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([minx, maxy])
                maxx_wgs, miny_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([maxx, miny])
                
                poly = Polygon([[minx_wgs, maxy_wgs], [maxx_wgs, maxy_wgs], [maxx_wgs, miny_wgs], [minx_wgs, miny_wgs], [minx_wgs, maxy_wgs]])
                element = {'geometry':mapping(poly), 'properties': {'id': i * cnum_tile + j, 'dataid': tileid, 'row':i, 'col':j}}
                layer.write(element) 
                
                minx_list.append(minx_wgs)
                maxy_list.append(maxy_wgs)
                maxx_list.append(maxx_wgs)
                miny_list.append(miny_wgs)
                             
    bb = {}
    bb["subtaskname"] = subtask_list
    bb["minx"] = minx_list
    bb["maxy"] = maxy_list
    bb["maxx"] = maxx_list
    bb["miny"] = miny_list      
 
    df = pd.DataFrame(bb)
    df.to_csv('/tmp/subtask_512_bbox_wgs.csv')

    
if __name__ == "__main__":
#     for region in region_dict.keys():
#         rasterfile = region_dict[region]['region']
#         dst_shp = os.path.join('/tmp',region+'_subbox.shp')
#         gen_subtask_bbox(rasterfile, region, dst_shp)
    
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
       lb_lat, dataexists, layerexists, the_geom FROM public.%s WHERE ST_Contains(the_geom, st_geomfromtext('%s',%s)) ORDER BY cloudcover ASC limit 10;'''%('metadata_landsat_oli_tirs', wkt, prj_epsg_int)
            print(region_query_sql)
            data = pg_src.getAll(region_query_sql)
            num =len(data)
            print(num)
#             trans_state, geom = gjson_geotrans_to_wgs84(geojson, projection)            
#             if trans_state==0:
#                 wkt = geom.ExportToWkt()
#                 region_query_sql = '''SELECT dataid, satellite, datatype, datadate, datadate_year, datadate_month, 
#        datadate_day, cloudcover, ct_long, ct_lat, lt_long, lt_lat, rt_long, rt_lat, rb_long, rb_lat, lb_long, 
#        lb_lat, dataexists, layerexists, the_geom FROM public.%s WHERE ST_Contains(the_geom, %s) ORDER BY cloudcover ASC limit 10;'''%('metadata_landsat_oli_tirs', f['geometry']['coordinates'][0])
#                 data = pg_src.getAll(region_query_sql)
#                 num =len(data)
#                 print(num)
                

                    
    

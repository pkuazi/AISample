import os,sys
from osgeo import gdal, ogr, osr
import fiona
from utils.geotrans import GeomTrans
from shapely.geometry import mapping, Polygon
import pandas as pd
import utils.pgsql as pgsql
import json
from dem_search_merge import region_search_dem,merge_all_dem

pg_src = pgsql.Pgsql("10.0.81.35", "2345", "postgres", "", "gscloud_metadata")

BLOCK_SIZE = 256
OVERLAP_SIZE = 13
AI_RESOLUTION = 30

region_dict = {'bj':{'region_tif':'bj.tif', 'year':[2001, 2003, 2004], 'images_key':'bj'},
               'cd':{'region_tif':'cd.tif', 'year':[1990, 2000, 2010, 2015], 'images_key':'cd_zjk'},
               'liangji':{'region_tif':'liangji.tif', 'year':[2015], 'images_key':'liangji'},
               'mws':{'region_tif': 'mws.tif', 'year':[1978, 2000, 2015], 'images_key':'mws'},
               'PD':{'region_tif': 'PD.tif', 'year':[1995, 2005, 2015], 'images_key':'PD'},
               'shanghai':{'region_tif':'shanghai.tif', 'year':[2006, 2009], 'images_key':'shanghai'},
               "sjz":{'region_tif': 'sjz.tif', 'year':[2013], 'images_key':'sjz'},
               'wuhan':{'region_tif':'wuhan.tif', 'year':[2015], 'images_key':'bj'},
               'xiaoshan':{'region_tif': 'xiaoshan.tif', 'year':[1996, 2001, 2006, 2013], 'images_key':'xiaoshan'},
               'yishui':{'region_tif':'yishui.tif', 'year':[1995, 2005, 2015], 'images_key':'yishui'},
               'zjk':{'region_tif': 'zjk.tif', 'year':[1990, 2000, 2010, 2015], 'images_key':'cd_zjk'},
               }
ROOT_PATH = '/mnt/rsimages/lulc/AISample'
region_tif_path = os.path.join(ROOT_PATH,'region_raster')
region_files = os.listdir(region_tif_path)
imageid_path = os.path.join(ROOT_PATH,'IMAGE')
imageids_file = os.path.join(imageid_path,'imageids.csv')
irrg_path = os.path.join(ROOT_PATH,'BANDS_IRRG')
irrg_tile_path = os.path.join(ROOT_PATH,'TILE_IRRG')
gt_path = os.path.join(ROOT_PATH,'GT')
gt_tile_path = os.path.join(ROOT_PATH,'TILE_GT')
dem_path = os.path.join(ROOT_PATH,'DEM')
dem_tile_path = os.path.join(ROOT_PATH,'TILE_DEM')

def get_imageids(images_key, year):
#     images_key is the images_key in region_dict, year is year in region_dict
    imageids_data = pd.read_csv(imageids_file)
    folder = images_key + '_' + str(year)
    imageids_df = imageids_data[imageids_data['folder'] == folder]
    imageids = imageids_df['dataid']
    idlist = imageids.tolist()
    return idlist


def gen_tile_bbox(region):
#     region is one of the region_dict.keys()
    region_tif = region_dict[region]['region_tif']
    region_file = os.path.join(region_tif_path, region_tif)
    print('the image is :', region_file)
    dataset = gdal.Open(region_file)
    if dataset is None:
        print("Failed to open file: " + region_file)
        sys.exit(1)
    band = dataset.GetRasterBand(1)
    xsize = dataset.RasterXSize
    ysize = dataset.RasterYSize
    proj = dataset.GetProjection()
    geotrans = dataset.GetGeoTransform()
    tile_gt = list(geotrans)
    noDataValue = band.GetNoDataValue()
    
    minx_wgs, maxy_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([geotrans[0], geotrans[3]])
    maxx_wgs, miny_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([geotrans[0]+xsize*geotrans[1], geotrans[3]+ysize*geotrans[5]])
    region_bbox=[minx_wgs, maxy_wgs,maxx_wgs, miny_wgs]
    
    rnum_tile = int((ysize - BLOCK_SIZE) / (BLOCK_SIZE - OVERLAP_SIZE)) + 1
    cnum_tile = int((xsize - BLOCK_SIZE) / (BLOCK_SIZE - OVERLAP_SIZE)) + 1
    print('the number of tile is :', rnum_tile * cnum_tile)
    
#     xoff_list = []
#     yoff_list = []
    wgs_bbox_list = []
    
    for i in range(rnum_tile + 1):
        print(i)
        for j in range(cnum_tile + 1):
            xoff = 0 + (BLOCK_SIZE - OVERLAP_SIZE) * j
            yoff = 0 + (BLOCK_SIZE - OVERLAP_SIZE) * i
            # the last column                 
            if j == cnum_tile:
                xoff = xsize - BLOCK_SIZE
            # the last row
            if i == rnum_tile:
                yoff = ysize - BLOCK_SIZE
            print("the row and column of tile is :", xoff, yoff)
            
#                 xoff_list.append(xoff)
#                 yoff_list.append(yoff)
               
            tile_gt[0] = geotrans[0] + xoff * geotrans[1]
            tile_gt[3] = geotrans[3] + yoff * geotrans[5]
            
            minx = tile_gt[0]
            maxy = tile_gt[3]
            maxx = tile_gt[0] + BLOCK_SIZE * geotrans[1]
            miny = tile_gt[3] + BLOCK_SIZE * geotrans[5]
            
            minx_wgs, maxy_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([minx, maxy])
            maxx_wgs, miny_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([maxx, miny])
            
            wgs_bbox_list.append([minx_wgs, maxy_wgs, maxx_wgs, miny_wgs, i, j])
           
                        
    return wgs_bbox_list,rnum_tile,cnum_tile,region_bbox


def tile_bbox_to_shp(wgs_bbox_list, region_tiles_shp):
     # schema is a dictory
    schema = {'geometry': 'Polygon', 'properties': {'id': 'int', 'row':'int', 'col':'int'} }
    with fiona.open(region_tiles_shp, mode='w', driver='ESRI Shapefile', schema=schema, crs='EPSG:4326', encoding='utf-8') as layer:
        for wgs_bbox in wgs_bbox_list:
            minx_wgs, maxy_wgs, maxx_wgs, miny_wgs, i, j = wgs_bbox[0], wgs_bbox[1], wgs_bbox[2], wgs_bbox[3], wgs_bbox[4], wgs_bbox[5]
            poly = Polygon([[minx_wgs, maxy_wgs], [maxx_wgs, maxy_wgs], [maxx_wgs, miny_wgs], [minx_wgs, miny_wgs], [minx_wgs, maxy_wgs]])
            element = {'geometry':mapping(poly), 'properties': {'id': i * cnum_tile + j, 'row':i, 'col':j}}
            layer.write(element) 
        layer.close()

        
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

def tiling_raster(rasterfile, wgs_bbox_list, dst_folder, n_bands, namestart, nameend):
    print('the image is :', rasterfile)
    dataset = gdal.Open(rasterfile)
    if dataset is None:
        print("Failed to open file: " + rasterfile)
#         sys.exit(1)
        return
    band = dataset.GetRasterBand(1)
    xsize = dataset.RasterXSize
    ysize = dataset.RasterYSize
    proj = dataset.GetProjection()
    geotrans = dataset.GetGeoTransform()
    gt = list(geotrans)
    noDataValue = band.GetNoDataValue()
    
    if geotrans[3]!= AI_RESOLUTION:
        print('the image %s needs resampling'%rasterfile)
        return
    
    for wgs_bbox in wgs_bbox_list:
        minx_wgs, maxy_wgs, maxx_wgs, miny_wgs, i, j = wgs_bbox[0], wgs_bbox[1], wgs_bbox[2], wgs_bbox[3], wgs_bbox[4], wgs_bbox[5]
        row = '0' + str(i)           
        col = '0' + str(j)  
        print('the current tiling location is ',i,j)
#         tile_name = region + str(year) + '_'+row[-2:] + col[-2:] + '_' + tileid + '.tif'
        tile_name = namestart + '_'+row[-2:] + col[-2:]+ nameend
        minx, maxy = GeomTrans('EPSG:4326', proj).transform_point([minx_wgs, maxy_wgs])
        maxx, miny = GeomTrans('EPSG:4326', proj).transform_point([maxx_wgs, miny_wgs])
        
        xoff = int((minx - geotrans[0]) / geotrans[1])
        yoff = int((maxy - geotrans[3]) / geotrans[5])
        
        if xoff<0 or yoff<0 or xoff+BLOCK_SIZE>xsize or yoff+BLOCK_SIZE>ysize:
            continue
        
        tile_data = dataset.ReadAsArray(xoff, yoff, BLOCK_SIZE, BLOCK_SIZE)
        
        gt[0] = minx
        gt[3] = maxy
        
        tile_file = os.path.join(dst_folder, tile_name)
#         xsize, ysize = tile_data[0].shape
        dst_format = 'GTiff'
        dst_nbands = n_bands
        dst_datatype = gdal.GDT_Float32
    
        driver = gdal.GetDriverByName(dst_format)
        dst_ds = driver.Create(tile_file, BLOCK_SIZE, BLOCK_SIZE, dst_nbands, dst_datatype)
        dst_ds.SetGeoTransform(gt)
        dst_ds.SetProjection(proj)
        dst_ds.SetNoDataValue(noDataValue)
        if dst_nbands==1:
            dst_ds.GetRasterBand(1).WriteArray(tile_data)
        else:
            for i in range(dst_nbands):
                dst_ds.GetRasterBand(i+1).WriteArray(tile_data[i])
        del dst_ds

    
if __name__ == "__main__":
# bj_2001: LT51230322001323BJC00  LT51230332001323BJC00
    for region in region_dict.keys():
        region_tiles_shp = os.path.join('/mnt/win/data/AISample/region_bbox/%s' % (region + '_subtiles.shp'))
        wgs_bbox_list, rnum, cnum, region_bbox = gen_tile_bbox(region)
        print('row,col: %s, %s'%(rnum,cnum))
        images_key = region_dict[region]['images_key']
        year_list = region_dict[region]['year']
        
        region_minx=region_bbox[0]
        region_miny=region_bbox[3]
        region_maxx=region_bbox[2]
        region_maxy=region_bbox[1]
        region_data = region_search_dem(region_miny, region_maxy, region_minx, region_maxx)
        region_dem_file = os.path.join(dem_path,region+'_dem.tif')
        merge_all_dem(region_data,region_dem_file)
    
        demfile = tiling_raster(region_dem_file, wgs_bbox_list, dem_tile_path, 1, region, '_dem.tif')
        
        for year in year_list:
            imageids = get_imageids(images_key=images_key, year=year)
            print(imageids)
            for image in imageids:
                rasterfile = os.path.join(irrg_path, image + '_IRRG.TIF')
                tiling_raster(rasterfile, wgs_bbox_list, irrg_tile_path, 3, region + '_' + str(year), image+'_.tif')
            gtfile = os.path.join(gt_path, region + '_' + str(year) + '.tif')
            tiling_raster(gtfile, wgs_bbox_list, gt_tile_path, 1, region + '_' + str(year),'_label.tif')
            
            

#         rasterfile = region_dict[region]['region']
#         dst_shp = os.path.join('/tmp',region+'_subbox.shp')
#         gen_subtask_bbox(rasterfile, region, dst_shp)
    

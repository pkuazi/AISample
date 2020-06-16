import os, sys
from osgeo import gdal, ogr, osr
import fiona
from utils.geotrans import GeomTrans
from utils.shp2json import Shp2Json
from shapely.geometry import mapping, Polygon
import pandas as pd
import utils.pgsql as pgsql
import json
# from image_search_merge import region_search_dem,merge_all_dem
import numpy as np
from gen_subtask_bbox import gen_tile_bbox, tile_bbox_to_shp
from shp_into_pgsql import tasktiles_shp_into_pgsql,gjson_geotrans_to_wgs84,get_curtime,get_taskid_by_tasktitle,get_wkt_by_tasktitle
from image_search_merge import region_query_tiles, query_tiles_by_tasktitle,region_search_dem,merge_all_dem
from utils.resampling import resampling

BLOCK_SIZE = 256
OVERLAP_SIZE = 13
RESOLUTION=30.0
# region_dict = {'bj':{'region_tif':'bj.tif', 'year':[2001, 2003, 2004], 'images_key':'bj'},
#                'cd':{'region_tif':'cd.tif', 'year':[1990, 2000, 2010, 2015], 'images_key':'cd_zjk'},
#                'liangji':{'region_tif':'liangji.tif', 'year':[2015], 'images_key':'liangji'},
#                'mws':{'region_tif': 'mws.tif', 'year':[1978, 2000, 2015], 'images_key':'mws'},
#                'PD':{'region_tif': 'PD.tif', 'year':[1995, 2005, 2015], 'images_key':'PD'},
#                'shanghai':{'region_tif':'shanghai.tif', 'year':[2006, 2009], 'images_key':'shanghai'},
#                "sjz":{'region_tif': 'sjz.tif', 'year':[2013], 'images_key':'sjz'},
#                'wuhan':{'region_tif':'wuhan.tif', 'year':[2015], 'images_key':'wuhan'},
#                'xiaoshan':{'region_tif': 'xiaoshan.tif', 'year':[1996, 2001, 2006, 2013], 'images_key':'xiaoshan'},
#                'yishui':{'region_tif':'yishui.tif', 'year':[1995, 2005, 2015], 'images_key':'yishui'},
#                'zjk':{'region_tif': 'zjk.tif', 'year':[1990, 2000, 2010, 2015], 'images_key':'cd_zjk'},
#                }
region_dict = {'bj':{ 'year':[2001, 2003, 2004], 'images_key':'bj'},
               'cd':{ 'year':[1990, 2000, 2010, 2015], 'images_key':'cd_zjk'},
               'liangji':{ 'year':[2015], 'images_key':'liangji'},
               'mws':{ 'year':[1978, 2000, 2015], 'images_key':'mws'},
               'PD':{ 'year':[1995, 2005, 2015], 'images_key':'PD'},
               'shanghai':{ 'year':[2006, 2009], 'images_key':'shanghai'},
               "sjz":{ 'year':[2013], 'images_key':'sjz'},
               'wuhan':{ 'year':[2015], 'images_key':'wuhan'},
               'xiaoshan':{ 'year':[1996, 2001, 2006, 2013], 'images_key':'xiaoshan'},
               'yishui':{ 'year':[1995, 2005, 2015], 'images_key':'yishui'},
               'zjk':{'year':[1990, 2000, 2010, 2015], 'images_key':'cd_zjk'},
               }
ROOT_PATH = '/mnt/rsimages/lulc/AISample'
# ROOT_PATH = '/mnt/win/data/AISample/'
# region_tif_path = os.path.join(ROOT_PATH, 'region_raster')
region_shp_path = os.path.join(ROOT_PATH,'region_shp')
region_bbox_path = os.path.join(ROOT_PATH, 'region_bbox')
if not os.path.exists(region_bbox_path):
    os.system('mkdir %s' % region_bbox_path)
# region_files = os.listdir(region_tif_path)
imageid_path = os.path.join(ROOT_PATH, 'IMAGE')
imageids_file = os.path.join(imageid_path, 'imageids.csv')

irrg_path = os.path.join(ROOT_PATH, 'BANDS_IRRG')
irrg_tile_path = os.path.join(ROOT_PATH, 'TILE_IRRG')
if not os.path.exists(irrg_tile_path):
    os.system('mkdir %s' % irrg_tile_path)
    
gt_path = os.path.join(ROOT_PATH, 'GT')
gt_tile_path = os.path.join(ROOT_PATH, 'TILE_GT')
if not os.path.exists(gt_tile_path):
    os.system('mkdir %s' % gt_tile_path)

dem_path = os.path.join(ROOT_PATH,'DEM')
dem_tile_path = os.path.join(ROOT_PATH,'TILE_DEM')
if not os.path.exists(dem_tile_path):
    os.system('mkdir %s'%dem_tile_path)
    
dem30_path = os.path.join(ROOT_PATH,'DEM30')
    
def get_imageids(images_key, year):
#     images_key is the images_key in region_dict, year is year in region_dict
    imageids_data = pd.read_csv(imageids_file)
    folder = images_key + '_' + str(year)
    imageids_df = imageids_data[imageids_data['folder'] == folder]
    imageids = imageids_df['dataid']
    idlist = imageids.tolist()
    print(idlist)
    return idlist

def wktlist_tiling_raster(rasterfile, wkt_list, dst_folder, n_bands, namestart, nameend):
    print('start tiling the image :', rasterfile)
    dataset = gdal.Open(rasterfile)
    if dataset is None:
        print("Failed to open file: " + rasterfile)
#         sys.exit(1)
        return
    
    geotrans = dataset.GetGeoTransform()
    gt = list(geotrans)
    
    band = dataset.GetRasterBand(1)
    xsize = dataset.RasterXSize
    ysize = dataset.RasterYSize
    proj = dataset.GetProjection()
    
    noDataValue = band.GetNoDataValue()
    dataType = band.DataType
      
    for wkt_poly in wkt_list:
        wgs_poly, i, j = wkt_poly[0], wkt_poly[1], wkt_poly[2]
        row = '0' + str(i)           
        col = '0' + str(j)  
        print('the current tiling location is ', i, j)
#         tile_name = region + str(year) + '_'+row[-2:] + col[-2:] + '_' + tileid + '.tif'
        tile_name = namestart + '_' + row[-2:] + col[-2:] + nameend
        poly = GeomTrans('EPSG:4326', proj).transform_wkt(wgs_poly)
       
        
        xoff = int((minx - geotrans[0]) / geotrans[1])
        yoff = int((maxy - geotrans[3]) / geotrans[5])
        
        if xoff < 0 or yoff < 0 or xoff + BLOCK_SIZE > xsize or yoff + BLOCK_SIZE > ysize:
            continue
        
        tile_data = dataset.ReadAsArray(xoff, yoff, BLOCK_SIZE, BLOCK_SIZE)
        
        gt[0] = minx
        gt[3] = maxy
    
        tile_file = os.path.join(dst_folder, tile_name)
#         xsize, ysize = tile_data[0].shape
        dst_format = 'GTiff'
        dst_nbands = n_bands
        dst_datatype = dataType
    
        driver = gdal.GetDriverByName(dst_format)
        dst_ds = driver.Create(tile_file, BLOCK_SIZE, BLOCK_SIZE, dst_nbands, dst_datatype)
        dst_ds.SetGeoTransform(gt)
        dst_ds.SetProjection(proj)

        if dst_nbands == 1:
#             tile_data[tile_data == noDataValue] = np.NaN
            dst_ds.GetRasterBand(1).WriteArray(tile_data)
        else:
            for i in range(dst_nbands):
#                 tile_data[i][tile_data[i] == noDataValue] = np.NaN
                dst_ds.GetRasterBand(i + 1).WriteArray(tile_data[i])
        del dst_ds     
def tiling_raster(rasterfile, wgs_bbox_list, dst_folder, n_bands, namestart, nameend):
    print('start tiling the image :', rasterfile)
    dataset = gdal.Open(rasterfile)
    if dataset is None:
        print("Failed to open file: " + rasterfile)
#         sys.exit(1)
        return
    
    geotrans = dataset.GetGeoTransform()
    gt = list(geotrans)
    
    band = dataset.GetRasterBand(1)
    xsize = dataset.RasterXSize
    ysize = dataset.RasterYSize
    proj = dataset.GetProjection()
    
    noDataValue = band.GetNoDataValue()
    dataType = band.DataType
      
    for wgs_bbox in wgs_bbox_list:
        minx_wgs, maxy_wgs, maxx_wgs, miny_wgs, i, j = wgs_bbox[0], wgs_bbox[1], wgs_bbox[2], wgs_bbox[3], wgs_bbox[4], wgs_bbox[5]
        row = '0' + str(i)           
        col = '0' + str(j)  
        print('the current tiling location is ', i, j)
#         tile_name = region + str(year) + '_'+row[-2:] + col[-2:] + '_' + tileid + '.tif'
        tile_name = namestart + '_' + row[-2:] + col[-2:] + nameend
        minx, maxy = GeomTrans('EPSG:4326', proj).transform_point([minx_wgs, maxy_wgs])
        maxx, miny = GeomTrans('EPSG:4326', proj).transform_point([maxx_wgs, miny_wgs])
        
        xoff = int((minx - geotrans[0]) / geotrans[1])
        yoff = int((maxy - geotrans[3]) / geotrans[5])
        
        if xoff < 0 or yoff < 0 or xoff + BLOCK_SIZE > xsize or yoff + BLOCK_SIZE > ysize:
            continue
        
        tile_data = dataset.ReadAsArray(xoff, yoff, BLOCK_SIZE, BLOCK_SIZE)
        
        gt[0] = minx
        gt[3] = maxy
    
        tile_file = os.path.join(dst_folder, tile_name)
#         xsize, ysize = tile_data[0].shape
        dst_format = 'GTiff'
        dst_nbands = n_bands
        dst_datatype = dataType
    
        driver = gdal.GetDriverByName(dst_format)
        dst_ds = driver.Create(tile_file, BLOCK_SIZE, BLOCK_SIZE, dst_nbands, dst_datatype)
        dst_ds.SetGeoTransform(gt)
        dst_ds.SetProjection(proj)

        if dst_nbands == 1:
#             tile_data[tile_data == noDataValue] = np.NaN
            dst_ds.GetRasterBand(1).WriteArray(tile_data)
        else:
            for i in range(dst_nbands):
#                 tile_data[i][tile_data[i] == noDataValue] = np.NaN
                dst_ds.GetRasterBand(i + 1).WriteArray(tile_data[i])
        del dst_ds
        

def sifting_tiling_grid(imageid, tileshp):
# by properties of imageid and cloud
    # imageid = os.path.split(rasterfile)[1][:-9]
    # print(imageid)
    with fiona.open(tileshp, 'r') as inp:
        projection = inp.crs_wkt
        
        wgs_bbox_list = []
        for f in inp:
            id = f['properties']['id']
            row = f['properties']['row']
            col = f['properties']['col']
            print(id)
            tile_imageid = f['properties']['imageid']
            tile_cloud = f['properties']['cloud']
            print(tile_imageid, tile_cloud)
            if tile_imageid == imageid and tile_cloud != 1:
                       
                points = list(f['geometry']['coordinates'][0])
                pts = np.array(points)
                minx = pts[:, 0].min()
                maxx = pts[:, 0].max()
                miny = pts[:, 1].min()
                maxy = pts[:, 1].max()

                minx_wgs, maxy_wgs = GeomTrans('EPSG:4326', projection).transform_point([minx, maxy])
                maxx_wgs, miny_wgs = GeomTrans('EPSG:4326', projection).transform_point([maxx, miny])
                
                wgs_bbox_list.append([minx_wgs, maxy_wgs, maxx_wgs, miny_wgs, int(row), int(col)])
    return wgs_bbox_list
def sifting_subtask_tile(task_title):
    import utils.pgsql as pgsql
    pg_src = pgsql.Pgsql("10.0.81.19", "9999","postgres", "", "gscloud_web")
    task_search_sql = '''SELECT id, geojson FROM public.mark_task where title='%s';'''%(task_title)
    data = pg_src.getAll(task_search_sql)
    taskid = data[0][0]
    task_region = data[0][1]
#     region_geom = task_region['geometry']
    subtask_update_sql='''UPDATE public.mark_subtask SET sid=1 where taskid='%s' and ST_Contains(st_geomfromtext(%s), st_geomfromtext(geojson));'''
    pg_src.update(subtask_update_sql, (taskid,task_region))
    subtask_search_sql = '''SELECT id FROM public.mark_subtask where taskid='%s' and ST_Contains(st_geomfromtext('%s'), st_geomfromtext(geojson));'''%(taskid,task_region)
#     subtask_search_sql = '''SELECT id FROM public.mark_subtask where taskid='%s' and ST_Intersects(st_geomfromtext('%s'), st_geomfromtext(geojson));'''%(taskid,task_region)
    data1 = pg_src.getAll(subtask_search_sql)
    num = len(data1)
    print(num)
    return num
    

def get_image_bbox_withoutnodata(imagefile, dst_shp):
                    # import dboxio
#                 with dboxio.Open(imagefile) as ds:
#                     proj = ds.GetProjectionRef()
#                     points = ds.TransformCorrds(ds.GetNoDataCorrds())
# 
#                     new_boundary_4326 = dboxio.transform_geom_as_geojson({"type": "Polygon", "coordinates": [[
#                         points[:2], points[2:4], points[4:6], points[6:8], points[:2]
#                     ]]}, proj, "EPSG:4326")
#     reference function  root / databox_core/libdboxdataset/dboxdatasetutils.cpp -- CPLErr Databox::DBoxFindCorrds(GDALDataset *dboxdataset, std::vector<int> &corrds)
    ds = gdal.Open(imagefile)
    if ds is None:
        print("Failed to open file: " + imagefile)
        return
    xsize = ds.RasterXSize
    ysize = ds.RasterYSize
    band = ds.GetRasterBand(1)
    noDataValue = band.GetNoDataValue()  # None
    noDataValue = 0
    
    proj = ds.GetProjectionRef()
    values = band.ReadAsArray()
    shape = values.shape
    geotrans = ds.GetGeoTransform()
    
    coords = []
#     left top corner
    for yoff in range(0, ysize - 1):
        for xoff in range(0, xsize - 1):
            ival = values[yoff][xoff]  # 0
            if ival != noDataValue:
                print(xoff, yoff)
                coords.append(xoff)
                coords.append(yoff)
                break
        else:
            continue
        break
    
#     left bottom corner
    for xoff in range(0, xsize - 1):
        for yoff in range(0, ysize - 1):
            ival = values[yoff][xoff]  #
            if ival != noDataValue:
                coords.append(xoff)
                coords.append(yoff)
                break
        else:
            continue
        break
#     right top corner
    for xoff in range(xsize - 1, 0, -1):
        for yoff in range(ysize - 1, 0, -1):
            ival = values[yoff][xoff]  #
            if ival != noDataValue:
                coords.append(xoff)
                coords.append(yoff)
                break
        else:
            continue
        break   
#     right bottom corner
    for yoff in range(ysize - 1, 0, -1):
        for xoff in range(xsize - 1, 0, -1):
            ival = values[yoff][xoff]  #
            if ival != noDataValue:
                coords.append(xoff)
                coords.append(yoff)
                break
        else:
            continue
        break
    
    ltx_wgs, lty_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([geotrans[0] + coords[0] * geotrans[1], geotrans[3] + coords[1] * geotrans[5]])
    lbx_wgs, lby_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([geotrans[0] + coords[2] * geotrans[1], geotrans[3] + coords[3] * geotrans[5]])
    rtx_wgs, rty_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([geotrans[0] + coords[4] * geotrans[1], geotrans[3] + coords[5] * geotrans[5]])
    rbx_wgs, rby_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([geotrans[0] + coords[6] * geotrans[1], geotrans[3] + coords[7] * geotrans[5]])
   
    geom_wkt = 'POLYGON ((%s %s,%s %s,%s %s,%s %s,%s %s))' % (ltx_wgs, lty_wgs, rtx_wgs, rty_wgs, rbx_wgs, rby_wgs, lbx_wgs, lby_wgs, ltx_wgs, lty_wgs)
    
    schema = {'geometry': 'Polygon', 'properties': {'id': 'int'} }
    with fiona.open(dst_shp, mode='w', driver='ESRI Shapefile', schema=schema, crs='EPSG:4326', encoding='utf-8') as layer:       
        poly = Polygon([[ltx_wgs, lty_wgs], [rtx_wgs, rty_wgs], [rbx_wgs, rby_wgs], [lbx_wgs, lby_wgs], [ltx_wgs, lty_wgs]])
        element = {'geometry':mapping(poly), 'properties': {'id': 1}}
        layer.write(element) 
        layer.close()
    
#     ogr.CreateGeometryFromWkt(geom_wkt)
    return geom_wkt

def task_update():
    pg_src = pgsql.Pgsql("10.0.81.19", "9999","postgres", "", "gscloud_web")
    for region in region_dict.keys():
        # region_tiles_shp = os.path.join(region_bbox_path,(region + '_subtiles.shp'))
            # region is one of the region_dict.keys()
        
        # print('row,col: %s, %s'%(rnum,cnum))
        images_key = region_dict[region]['images_key']
        year_list = region_dict[region]['year']
      
        for year in year_list:
            
            task_title = region + '_' + str(year)
          
            region_shp=os.path.join(region_shp_path, region+'_wgs.geojson')
            
            print(region_shp)
            with open(region_shp, "r") as f:    #打开文件
                data = f.read()   #读
                
            task_geojson = json.loads(data)
            geom_json = json.dumps(task_geojson['features'][0]['geometry'])

            geom = ogr.CreateGeometryFromJson(geom_json)  
            task_wkt = geom.ExportToWkt()
            
            gtfile = os.path.join(gt_path, region + '_' + str(year) + '.tif')

            imageids = get_imageids(images_key=images_key, year=year)
            
            task_update_sql = '''UPDATE public.mark_task SET geojson=%s, gtfile=%s,image=%s where title=%s;'''
            pg_src.update(task_update_sql, (task_wkt, gtfile, imageids, task_title))
def proj_image(imagefile, gt_file, outfile):
    ds = gdal.Open(region_dem_file)
    in_proj = ds.GetProjection()
    gt_ds = gdal.Open(gt_file)
    out_proj = gt_ds.GetProjection()
    
    inSpatialRef = osr.SpatialReference()
    inSpatialRef.SetFromUserInput(in_proj)
      
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.SetFromUserInput(out_proj)       

    if inSpatialRef.IsSame(self.outSpatialRef) == 0:
        proj_cmd = "gdalwarp -t_srs %s %s %s" % (out_proj, imagefile, outfile) 
        os.system(proj_cmd)
    else:
        outfile=imagefile
    
    
def tiling_for_dataset():
    for region in region_dict.keys():
        # region_tiles_shp = os.path.join(region_bbox_path,(region + '_subtiles.shp'))
            # region is one of the region_dict.keys()
          
        # print('row,col: %s, %s'%(rnum,cnum))
        images_key = region_dict[region]['images_key']
        year_list = region_dict[region]['year']
                   
        for year in year_list:
            task_title = region + '_' + str(year)

            gtfile = os.path.join(gt_path, region + '_' + str(year) + '.tif')
            
            tiles_list = query_tiles_by_tasktitle(task_title)
# #             DictRow: ['bj_2001_02_03', 'POLYGON ((116.414767331843 40.4163919701254,116.505793909987 40.4163919701254,116.505793909987 40.3476266055874,116.414767331843 40.3476266055874,116.414767331843 40.4163919701254))', 'LT51230322001323BJC00']
            for i in range(len(tiles_list)):
                guid = tiles_list[i][0]
                geojson = tiles_list[i][1]
                imageid = tiles_list[i][2]
                sid = tiles_list[i][3]
                if imageid is None or sid!=1:
                    continue
#                 
                imagefile = os.path.join(irrg_path, imageid + '_IRRG.TIF')
                outfile = '/tmp/%s_IRRG.TIF'%(imageid)
                proj_image(imagefile, gt_file, outfile)
                demfile = os.path.join(dem30_path,region+'_'+str(year)+'_dem.tif')
#                 
                row=int(guid[-5:-3])
                col=int(guid[-2:])
                  
                geom = ogr.CreateGeometryFromWkt(geojson)
                minx_wgs, maxx_wgs, miny_wgs,maxy_wgs =geom.GetEnvelope()
                wgs_bbox_list = []
                wgs_bbox_list.append([minx_wgs, maxy_wgs, maxx_wgs, miny_wgs, row, col])  
                tiling_raster(outfile, wgs_bbox_list, irrg_tile_path,  3, region + '_' + str(year), '_'+imageid+'.tif')
                tiling_raster(gtfile,wgs_bbox_list, gt_tile_path,  1, region + '_' + str(year),'_label.tif')
                tiling_raster(demfile,wgs_bbox_list, dem_tile_path,  1, region + '_' + str(year),'_dem.tif')
def gen_subtask():  
    pg_src = pgsql.Pgsql("10.0.81.19", "9999","postgres", "", "gscloud_web")
    for region in region_dict.keys():
        # region_tiles_shp = os.path.join(region_bbox_path,(region + '_subtiles.shp'))
            # region is one of the region_dict.keys()

          
        # print('row,col: %s, %s'%(rnum,cnum))
        images_key = region_dict[region]['images_key']
        year_list = region_dict[region]['year']
                   
        for year in year_list:
#             subtask--tiles into pgsql
            task_title = region + '_' + str(year)
            imageids = get_imageids(images_key=images_key, year=year)
            gtfile = os.path.join(gt_path, region + '_' + str(year) + '.tif')
            
            tile_shp = os.path.join(region_bbox_path,(region + '_'+str(year)+'_'+'tiles.shp'))
            wgs_bbox_list, rnum, cnum, region_bbox = gen_tile_bbox(gtfile,BLOCK_SIZE, OVERLAP_SIZE)
            tile_bbox_to_shp(wgs_bbox_list, rnum, cnum, tile_shp)
#             tasktiles_shp_into_pgsql(task_title, tile_shp, imageids)
            sql = "select id from public.mark_task where title='%s' " % (task_title)
            datas = pg_src.getAll(sql)
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
                        
                        sql = "select * from public.mark_subtask where guid='%s' " % (guid)
                        datas = pg_src.getAll(sql)
                    
                        if len(datas) == 0:
                            pg_src.update(insert_sql, (guid, taskid, ctime, wkt))
                            print("insert subtask tile of ", guid)
                        else:
                            pg_src.update(update_sql, (guid, taskid, ctime, wkt))
                            print("insert subtask tile of ", guid)      

def gen_random_samplepts():
    for region in region_dict.keys():
        # region_tiles_shp = os.path.join(region_bbox_path,(region + '_subtiles.shp'))
            # region is one of the region_dict.keys()

          
        # print('row,col: %s, %s'%(rnum,cnum))
        images_key = region_dict[region]['images_key']
        year_list = region_dict[region]['year']
          
# #         region_minx=region_bbox[0]
# #         region_miny=region_bbox[3]
# #         region_maxx=region_bbox[2]
# #         region_maxy=region_bbox[1]
# #         region_data = region_search_dem(region_miny, region_maxy, region_minx, region_maxx)
# #         region_dem_file = os.path.join(dem_path,region+'_dem.tif')
# #         merge_all_dem(region_data,region_dem_file)
# #     
# #         demfile = tiling_raster(region_dem_file, wgs_bbox_list, dem_tile_path, 1, region, '_dem.tif')
          
        for year in year_list:
            task_title = region + '_' + str(year)

            gtfile = os.path.join(gt_path, region + '_' + str(year) + '.tif')
            
            tiles_list = query_tiles_by_tasktitle(task_title)
# #             DictRow: ['bj_2001_02_03', 'POLYGON ((116.414767331843 40.4163919701254,116.505793909987 40.4163919701254,116.505793909987 40.3476266055874,116.414767331843 40.3476266055874,116.414767331843 40.4163919701254))', 'LT51230322001323BJC00']
            for i in range(len(tiles_list)):
                guid = tiles_list[i][0]
                geojson = tiles_list[i][1]
                imageid = tiles_list[i][2]
                sid = tiles_list[i][3]
                if imageid is None or sid!=1:
                    continue
                geom = ogr.CreateGeometryFromWkt(geojson)
                minx_wgs, maxx_wgs, miny_wgs,maxy_wgs =geom.GetEnvelope()
                pt_x = random.uniform(minx_wgs, maxx_wgs)
                pt_y = random.uniform(miny_wgs, maxy_wgs)
                                            
def subtask_update_imageid_sid():
    pg_src = pgsql.Pgsql("10.0.81.19", "9999","postgres", "", "gscloud_web")
    for region in region_dict.keys():
        # region_tiles_shp = os.path.join(region_bbox_path,(region + '_subtiles.shp'))
            # region is one of the region_dict.keys()

          
        # print('row,col: %s, %s'%(rnum,cnum))
        images_key = region_dict[region]['images_key']
        year_list = region_dict[region]['year']
                   
        for year in year_list:
#             subtask--tiles into pgsql
            task_title = region + '_' + str(year)
            taskid = get_taskid_by_tasktitle(task_title)
            imageids = get_imageids(images_key=images_key, year=year)
            for image in reversed(imageids):
#                 find all the tiles contained in the image bbox
                imagefile = os.path.join(irrg_path, image + '_IRRG.TIF')
                imagebbox = get_image_bbox_withoutnodata(imagefile,'/tmp/%s.shp'%image)
                
                tile_update_imageid_sql = '''UPDATE public.mark_subtask SET imageid=%s where taskid='%s' and ST_Contains(st_geomfromtext(%s), geojson);'''
                pg_src.update(tile_update_imageid_sql, (image, taskid, imagebbox))
            
            task_region=get_wkt_by_tasktitle(task_title)
            subtask_update_sql='''UPDATE public.mark_subtask SET sid=1 where taskid='%s' and ST_Contains(st_geomfromtext(%s), st_geomfromtext(geojson));'''
            pg_src.update(subtask_update_sql, (taskid,task_region))
             
def check_image_resolution(imagefile):
    dataset = gdal.Open(imagefile)
    if dataset is None:
        print("Failed to open file: " + imagefile)
#         sys.exit(1)
        return
    
    geotrans = dataset.GetGeoTransform()
    gt = list(geotrans)
    
    if gt[1]!=RESOLUTION:
        target_file = '/tmp/birrg_30.tif'
        resampling(imagefile, target_file, scale=gt[1]/RESOLUTION)
        rm_cmd = 'rm -rf %s'%imagefile
        print(rm_cmd)
        os.system(rm_cmd)
        mv_cmd = 'mv %s %s'%(target_file,imagefile)
        print(mv_cmd)
        os.system(mv_cmd)
        
def resample_dem(region_dem_file, gt_file, outfile):
    ds = gdal.Open(region_dem_file)
    src_srs = ds.GetProjection()
    gt_ds = gdal.Open(gt_file)
    dst_srs = gt_ds.GetProjection()
    proj_cmd = "gdalwarp -t_srs %s %s %s" % (dst_srs, region_dem_file, outfile) 
    os.system(proj_cmd)
    check_image_resolution(outfile)

def process_dem():
    for region in region_dict.keys():
        year_list = region_dict[region]['year']          
        for year in year_list:
            region_dem_file = os.path.join(dem_path,region+'_'+str(year)+'_dem.tif')
            outfile = os.path.join(dem30_path,region+'_'+str(year)+'_dem.tif')
#             merge_all_dem(region_data,region_dem_file)
            gtfile = os.path.join(gt_path, region + '_' + str(year) + '.tif')
            resample_dem(region_dem_file, gtfile,outfile)
            
def merge_dem():
    for region in region_dict.keys():
        year_list = region_dict[region]['year']          
        for year in year_list:
            gtfile = os.path.join(gt_path, region + '_' + str(year) + '.tif')
            wgs_bbox_list, rnum, cnum, region_bbox = gen_tile_bbox(gtfile,BLOCK_SIZE, OVERLAP_SIZE)
            region_minx=region_bbox[0]
            region_miny=region_bbox[3]
            region_maxx=region_bbox[2]
            region_maxy=region_bbox[1]
            region_data = region_search_dem(region_miny, region_maxy, region_minx, region_maxx)
            region_dem_file = os.path.join(dem_path,region+'_'+str(year)+'_dem.tif')
            outfile = os.path.join(dem30_path,region+'_'+str(year)+'_dem.tif')
            merge_all_dem(region_data,region_dem_file)
            
def process_irrg():
    irrg_files = os.listdir(irrg_path)
    for irrg_file in irrg_files:
        if irrg_file.endswith('_IRRG.TIF'):
            imagefile = os.path.join(irrg_path,irrg_file)
            check_image_resolution(imagefile)
if __name__ == "__main__":
#     gen_subtask()
#     process_dem()
#     subtask_update_imageid_sid()
    tiling_for_dataset()
#     sql = '''select geojson, imageid from mark_subtask where guid like 'mws_1978_45_24';'''
#     data = pg_src.getAll(sql)
#     geojson = data[0][0]
#     imageid = data[0][1]
#     imagefile = os.path.join(irrg_path, imageid + '_IRRG.TIF')
#     row=45
#     col=24
#     geom = ogr.CreateGeometryFromWkt(geojson)
#     minx_wgs, maxx_wgs, miny_wgs,maxy_wgs =geom.GetEnvelope()
#     wgs_bbox_list = []
#     wgs_bbox_list.append([minx_wgs, maxy_wgs, maxx_wgs, miny_wgs, row, col])  
#     tiling_raster(imagefile, wgs_bbox_list, irrg_tile_path,  3, region + '_' + str(year), '_'+imageid+'.tif')
                 
# # bj_2001: LT51230322001323BJC00  LT51230332001323BJC00
#     pg_src = pgsql.Pgsql("10.0.81.19", "9999","postgres", "", "gscloud_web")
#     num_tiles = 0
#     for region in region_dict.keys():
#         images_key = region_dict[region]['images_key']
#         year_list = region_dict[region]['year']          
#         for year in year_list:
#             imageids = get_imageids(images_key=images_key, year=year)
#             subtask--tiles into pgsql
#             tile_shp = os.path.join(region_bbox_path,(region + '_'+str(year)+'_'+'tiles.shp'))
#             wgs_bbox_list, rnum, cnum, region_bbox = gen_tile_bbox(region_file,BLOCK_SIZE, OVERLAP_SIZE)

#             tiling_raster(region_dem_file, wgs_bbox_list, dem_tile_path,  1, region + '_' + str(year), '_'+'dem'+'.tif')
# #             
# #             tasktiles_shp_into_pgsql(task_title, tile_shp, imageids)
# #             tile_bbox_to_shp(wgs_bbox_list, rnum, cnum, tile_shp)
# #             if not os.path.exists(tile_shp):
# #                 print('the tiling shapefile does not exists')
# #                 continue
#                       
#             task_title = region + '_' + str(year)
# #             num = sifting_subtask_tile(task_title)
# #             num_tiles +=num
# #     print('the totle number of tiles is ',num_tiles)
# # #             wgs_bbox_list, rnum, cnum, region_bbox = gen_subtask_in_db(task_title,BLOCK_SIZE, OVERLAP_SIZE)
# # #             
# # #             print(task_title)
#             gtfile = os.path.join(gt_path, region + '_' + str(year) + '.tif')
# # #             
# #             region_shp=os.path.join(region_shp_path, region+'_wgs.geojson')
# #             
# #             print(region_shp)
# #             with open(region_shp, "r") as f:    #打开文件
# #                 data = f.read()   #读
# #                 
# #             task_geojson = json.loads(data)
# #             geom_json = json.dumps(task_geojson['features'][0]['geometry'])
# # 
# #             geom = ogr.CreateGeometryFromJson(geom_json)  
# #             task_wkt = geom.ExportToWkt()
# # # 
# #             task_update_sql = '''UPDATE public.mark_task SET geojson=%s, gtfile=%s where title=%s;'''
# #             pg_src.update(task_update_sql, (task_wkt, gtfile, task_title))
#             
#             tiles_list = query_tiles_by_tasktitle(task_title)
# # #             DictRow: ['bj_2001_02_03', 'POLYGON ((116.414767331843 40.4163919701254,116.505793909987 40.4163919701254,116.505793909987 40.3476266055874,116.414767331843 40.3476266055874,116.414767331843 40.4163919701254))', 'LT51230322001323BJC00']
#             for i in range(len(tiles_list)):
#                 guid = tiles_list[i][0]
#                 geojson = tiles_list[i][1]
#                 imageid = tiles_list[i][2]
#                 sid = tiles_list[i][3]
#                 if imageid is None or sid!=1:
#                     continue
# #                 
#                 imagefile = os.path.join(irrg_path, imageid + '_IRRG.TIF')
# #                 
#                 row=int(guid[-5:-3])
#                 col=int(guid[-2:])
#                  
#                 geom = ogr.CreateGeometryFromWkt(geojson)
#                 minx_wgs, maxx_wgs, miny_wgs,maxy_wgs =geom.GetEnvelope()
#                 wgs_bbox_list = []
#                 wgs_bbox_list.append([minx_wgs, maxy_wgs, maxx_wgs, miny_wgs, row, col])          

            
#             for image in reversed(imageids):
# #                 find all the tiles contained in the image bbox
#                 imagefile = os.path.join(irrg_path, image + '_IRRG.TIF')
#                 imagebbox = get_image_bbox_withoutnodata(imagefile,'/tmp/%s.shp'%image)
#                 
#                 region_query_tiles(image, imagebbox, task_title)
#             srcfiles1 = ''
#             dstfile_path = '/tmp/%s_%s.TIF'%(region,year)
#             for image in reversed(imageids):
#                 srcfiles1 = srcfiles1+os.path.join(irrg_path, image + '_IRRG.TIF')
#                 srcfiles1 = srcfiles1+' '
#                 print('the image to be tiling is',rasterfile)
                # wgs_bbox_list = sifting_tiling_grid(image, tile_shp)
#                 tiling_raster(imagefile, wgs_bbox_list, irrg_tile_path,  3, region + '_' + str(year), '_'+imageid+'.tif')
#                 tiling_raster(gtfile,wgs_bbox_list, gt_tile_path,  1, region + '_' + str(year),'_label.tif')
#             merge_cmd1 = 'gdalwarp -srcnodata None %s %s'%(srcfiles1, dstfile_path)
#             os.system(merge_cmd1)                 
                # tiling_raster(rasterfile, wgs_bbox_list, irrg_tile_path, 3, region + '_' + str(year), '_'+image+'.tif')
           
            # tiling_raster(gtfile, wgs_bbox_list, gt_tile_path, 1, region + '_' + str(year),'_label.tif')  
    

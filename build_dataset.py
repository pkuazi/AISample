import os,sys
from osgeo import gdal, ogr, osr
import fiona
from utils.geotrans import GeomTrans
from shapely.geometry import mapping, Polygon
import pandas as pd
import utils.pgsql as pgsql
import json
# from image_search_merge import region_search_dem,merge_all_dem
import numpy as np
from gen_subtask_bbox import gen_tile_bbox,tile_bbox_to_shp
from shp_into_pgsql import tasktiles_shp_into_pgsql


BLOCK_SIZE = 256
OVERLAP_SIZE = 13

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
# ROOT_PATH = '/mnt/rsimages/lulc/AISample'
ROOT_PATH = '/mnt/win/data/AISample/'
region_tif_path = os.path.join(ROOT_PATH,'region_raster')
region_bbox_path = os.path.join(ROOT_PATH,'region_bbox')
if not os.path.exists(region_bbox_path):
    os.system('mkdir %s'%region_bbox_path)
region_files = os.listdir(region_tif_path)
imageid_path = os.path.join(ROOT_PATH,'IMAGE')
imageids_file = os.path.join(imageid_path,'imageids.csv')
irrg_path = os.path.join(ROOT_PATH,'BANDS_IRRG')
irrg_tile_path = os.path.join(ROOT_PATH,'TILE_IRRG')
if not os.path.exists(irrg_tile_path):
    os.system('mkdir %s'%irrg_tile_path)
gt_path = os.path.join(ROOT_PATH,'GT')
gt_tile_path = os.path.join(ROOT_PATH,'TILE_GT')

if not os.path.exists(gt_tile_path):
    os.system('mkdir %s'%gt_tile_path)
# dem_path = os.path.join(ROOT_PATH,'DEM')
# dem_tile_path = os.path.join(ROOT_PATH,'TILE_DEM')
# if not os.path.exists(dem_tile_path):
#     os.system('mkdir %s'%dem_tile_path)
def get_imageids(images_key, year):
#     images_key is the images_key in region_dict, year is year in region_dict
    imageids_data = pd.read_csv(imageids_file)
    folder = images_key + '_' + str(year)
    imageids_df = imageids_data[imageids_data['folder'] == folder]
    imageids = imageids_df['dataid']
    idlist = imageids.tolist()
    return idlist
        
def tiling_raster(rasterfile, wgs_bbox_list, dst_folder, n_bands, namestart, nameend):
    print('start tiling the image :', rasterfile)
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

        if dst_nbands==1:
            tile_data[tile_data == noDataValue] = noDataValue
            dst_ds.GetRasterBand(1).WriteArray(tile_data)
        else:
            for i in range(dst_nbands):
                tile_data[i][tile_data[i] == noDataValue] = -9999
                dst_ds.GetRasterBand(i+1).WriteArray(tile_data[i])
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
            row= f['properties']['row']
            col= f['properties']['col']
            print(id)
            tile_imageid = f['properties']['imageid']
            tile_cloud = f['properties']['cloud']
            print(tile_imageid, tile_cloud)
            if tile_imageid == imageid and tile_cloud !=1:
                       
                points = list(f['geometry']['coordinates'][0])
                pts = np.array(points)
                minx = pts[:,0].min()
                maxx = pts[:,0].max()
                miny = pts[:,1].min()
                maxy = pts[:,1].max()

                minx_wgs, maxy_wgs = GeomTrans('EPSG:4326', projection).transform_point([minx, maxy])
                maxx_wgs, miny_wgs = GeomTrans('EPSG:4326', projection).transform_point([maxx, miny])
                
                
                wgs_bbox_list.append([minx_wgs, maxy_wgs, maxx_wgs, miny_wgs, int(row), int(col)])
    return wgs_bbox_list

def get_image_bbox_withoutnodata(imagefile,dst_shp):
#     reference function  root / databox_core/libdboxdataset/dboxdatasetutils.cpp -- CPLErr Databox::DBoxFindCorrds(GDALDataset *dboxdataset, std::vector<int> &corrds)
    ds = gdal.Open(imagefile)
    if ds is None:
        print("Failed to open file: " + imagefile)
        return
    xsize = ds.RasterXSize
    ysize = ds.RasterYSize
    band = ds.GetRasterBand(1)
    noDataValue = band.GetNoDataValue()#None
    noDataValue=0
    
    proj = ds.GetProjectionRef()
    values = band.ReadAsArray()
    shape =values.shape
    geotrans = ds.GetGeoTransform()
    
    coords = []
#     left top corner
    for yoff in range(0,ysize-1):
        for xoff in range(0,xsize-1):
            ival = values[yoff][xoff]#0
            if ival != noDataValue:
                print(xoff,yoff)
                coords.append(xoff)
                coords.append(yoff)
                break
        else:
            continue
        break
    
#     left bottom corner
    for xoff in range(0,xsize-1):
        for yoff in range(0,ysize-1):
            ival = values[yoff][xoff]#
            if ival != noDataValue:
                coords.append(xoff)
                coords.append(yoff)
                break
        else:
            continue
        break
#     right top corner
    for xoff in range(xsize-1, 0, -1):
        for yoff in range(ysize-1, 0, -1):
            ival = values[yoff][xoff]#
            if ival != noDataValue:
                coords.append(xoff)
                coords.append(yoff)
                break
        else:
            continue
        break   
#     right bottom corner
    for yoff in range(ysize-1, 0, -1):
        for xoff in range(xsize-1, 0, -1):
            ival = values[yoff][xoff]#
            if ival != noDataValue:
                coords.append(xoff)
                coords.append(yoff)
                break
        else:
            continue
        break
    
    ltx_wgs, lty_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([geotrans[0]+ coords[0]*geotrans[1], geotrans[3]+coords[1]*geotrans[5]])
    lbx_wgs, lby_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([geotrans[0]+ coords[2]*geotrans[1], geotrans[3]+coords[3]*geotrans[5]])
    rtx_wgs, rty_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([geotrans[0]+ coords[4]*geotrans[1], geotrans[3]+coords[5]*geotrans[5]])
    rbx_wgs, rby_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([geotrans[0]+ coords[6]*geotrans[1], geotrans[3]+coords[7]*geotrans[5]])
   
    geom_wkt = 'POLYGON ((%s %s,%s %s,%s %s,%s %s,%s %s))'%(ltx_wgs, lty_wgs,rtx_wgs, rty_wgs,rbx_wgs, rby_wgs,lbx_wgs, lby_wgs,ltx_wgs, lty_wgs)
    
    schema = {'geometry': 'Polygon', 'properties': {'id': 'int'} }
    with fiona.open(dst_shp, mode='w', driver='ESRI Shapefile', schema=schema, crs='EPSG:4326', encoding='utf-8') as layer:       
        poly = Polygon([[ltx_wgs, lty_wgs], [rtx_wgs, rty_wgs], [rbx_wgs, rby_wgs], [lbx_wgs, lby_wgs], [ltx_wgs, lty_wgs]])
        element = {'geometry':mapping(poly), 'properties': {'id': 1}}
        layer.write(element) 
        layer.close()
    
#     ogr.CreateGeometryFromWkt(geom_wkt)
    return geom_wkt
if __name__ == "__main__":
# bj_2001: LT51230322001323BJC00  LT51230332001323BJC00

    for region in region_dict.keys():
        # region_tiles_shp = os.path.join(region_bbox_path,(region + '_subtiles.shp'))
            # region is one of the region_dict.keys()
        region_tif = region_dict[region]['region_tif']
        region_file = os.path.join(region_tif_path, region_tif)
        
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
#             subtask--tiles into pgsql
#             tile_shp = os.path.join(region_bbox_path,(region + '_'+str(year)+'_'+'tiles.shp'))
#             wgs_bbox_list, rnum, cnum, region_bbox = gen_tile_bbox(region_file,BLOCK_SIZE, OVERLAP_SIZE)
#             tile_bbox_to_shp(wgs_bbox_list, rnum, cnum, tile_shp)
#             if not os.path.exists(tile_shp):
#                 print('the tiling shapefile does not exists')
#                 continue
            
            imageids = get_imageids(images_key=images_key, year=year)
#             task_title= region + '_'+str(year)
#             tasktiles_shp_into_pgsql(task_title, tile_shp, imageids)

            # gtfile = os.path.join(gt_path, region + '_' + str(year) + '.tif')
            for image in reversed(imageids):
#                 find all the tiles contained in the image bbox
                imagefile = os.path.join(irrg_path, image + '_IRRG.TIF')
                imagebbox = get_image_bbox_withoutnodata(imagefile,'/tmp/%s.shp'%image)
                # import dboxio
#                 with dboxio.Open(imagefile) as ds:
#                     proj = ds.GetProjectionRef()
#                     points = ds.TransformCorrds(ds.GetNoDataCorrds())
# 
#                     new_boundary_4326 = dboxio.transform_geom_as_geojson({"type": "Polygon", "coordinates": [[
#                         points[:2], points[2:4], points[4:6], points[6:8], points[:2]
#                     ]]}, proj, "EPSG:4326")
#             for image in imageids:
#                 
#                 print('the image to be tiling is',rasterfile)
                # wgs_bbox_list = sifting_tiling_grid(image, tile_shp)
                # tiling_raster(rasterfile, wgs_bbox_list, irrg_tile_path,  3, region + '_' + str(year), '_'+image+'.tif')
                # tiling_raster( gtfile,wgs_bbox_list, gt_tile_path,  1, region + '_' + str(year),'_label.tif')
                            
                # tiling_raster(rasterfile, wgs_bbox_list, irrg_tile_path, 3, region + '_' + str(year), '_'+image+'.tif')
           
            # tiling_raster(gtfile, wgs_bbox_list, gt_tile_path, 1, region + '_' + str(year),'_label.tif')  


    

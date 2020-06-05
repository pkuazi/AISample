import os,sys
from osgeo import gdal, ogr, osr
import fiona
from utils.geotrans import GeomTrans
from shapely.geometry import mapping, Polygon
import pandas as pd
import utils.pgsql as pgsql
import json
from image_search_merge import region_search_dem,merge_all_dem
import numpy as np
from gen_subtask_bbox import gen_tile_bbox

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
ROOT_PATH = '/mnt/rsimages/lulc/AISample'
region_tif_path = os.path.join(ROOT_PATH,'region_raster')
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
            tile_data[tile_data == noDataValue] = -9999
            dst_ds.GetRasterBand(1).WriteArray(tile_data)
        else:
            for i in range(dst_nbands):
                tile_data[i][tile_data[i] == noDataValue] = -9999
                dst_ds.GetRasterBand(i+1).WriteArray(tile_data[i])
        del dst_ds

    
if __name__ == "__main__":
# bj_2001: LT51230322001323BJC00  LT51230332001323BJC00
    for region in region_dict.keys():
        region_tiles_shp = os.path.join('/mnt/win/data/AISample/region_bbox/%s' % (region + '_subtiles.shp'))
        #     region is one of the region_dict.keys()
        region_tif = region_dict[region]['region_tif']
        region_file = os.path.join(region_tif_path, region_tif)
        wgs_bbox_list, rnum, cnum, region_bbox = gen_tile_bbox(region_file,BLOCK_SIZE, OVERLAP_SIZE)
        print('row,col: %s, %s'%(rnum,cnum))
        images_key = region_dict[region]['images_key']
        year_list = region_dict[region]['year']
        
#         region_minx=region_bbox[0]
#         region_miny=region_bbox[3]
#         region_maxx=region_bbox[2]
#         region_maxy=region_bbox[1]
#         region_data = region_search_dem(region_miny, region_maxy, region_minx, region_maxx)
#         region_dem_file = os.path.join(dem_path,region+'_dem.tif')
#         merge_all_dem(region_data,region_dem_file)
#     
#         demfile = tiling_raster(region_dem_file, wgs_bbox_list, dem_tile_path, 1, region, '_dem.tif')
        
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
    

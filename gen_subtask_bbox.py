from osgeo import gdal, ogr
import os
from utils.geotrans import GeomTrans
from shapely.geometry import mapping, Polygon

BLOCK_SIZE=256
OVERLAP_SIZE=13

def gen_tile_bbox(region_file,BLOCK_SIZE,OVERLAP_SIZE):
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
            
            data = band.ReadAsArray(xoff, yoff, BLOCK_SIZE, BLOCK_SIZE)
            if np.all(data == noDataValue):
                continue
            
               
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
        
def gen_subtask_bbox_shp(rasterfile, tileid, dst_shp):
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
                
                # the last column                 
                if j == cnum_tile:
                    maxx = geotrans[0] + xsize * geotrans[1]
                    minx = maxx - BLOCK_SIZE * geotrans[1]
                # the last row
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

def gen_subtask_bbox(rasterfile,imageid):
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
    gt=list(geotrans)
    noDataValue = band.GetNoDataValue()
    
    rnum_tile=int((ysize-BLOCK_SIZE)/(BLOCK_SIZE-OVERLAP_SIZE))+1
    cnum_tile=int((xsize-BLOCK_SIZE)/(BLOCK_SIZE-OVERLAP_SIZE))+1
    print('the number of tile is :',rnum_tile*cnum_tile)
    import pandas as pd
    subtask_list=[]
    minx_list=[]
    maxy_list=[]
    maxx_list=[]
    miny_list=[]
    
    for i in range(rnum_tile):
        print(i)
        for j in range(cnum_tile):
            xoff=0+(BLOCK_SIZE-OVERLAP_SIZE)*j
            yoff=0+(BLOCK_SIZE-OVERLAP_SIZE)*i
            print("the row and column of tile is :", xoff, yoff)

            gt[0]=geotrans[0]+xoff*geotrans[1]
            gt[3]=geotrans[3]+yoff*geotrans[5]
            
            subtask_list.append(imageid+str(i)+'_'+str(j))
            
            minx=gt[0]
            maxy=gt[3]
            maxx=gt[0]+BLOCK_SIZE*geotrans[1]
            miny=gt[3]+BLOCK_SIZE*geotrans[5]
            
            minx_wgs, maxy_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([minx,maxy])
            maxx_wgs, miny_wgs = GeomTrans(proj, 'EPSG:4326').transform_point([maxx,miny])
            
            minx_list.append(minx_wgs)
            maxy_list.append(maxy_wgs)
            maxx_list.append(maxx_wgs)
            miny_list.append(miny_wgs)
                             
    bb = {}
    bb["subtaskname"]=subtask_list
    bb["minx"]=minx_list
    bb["maxy"]=maxy_list
    bb["maxx"]=maxx_list
    bb["miny"]=miny_list      
 
    df=pd.DataFrame(bb)
    df.to_csv('/tmp/subtask_512_bbox_wgs.csv')

if __name__ == '__main__':   
    task_data = '/mnt/win/data/sample_image/xiaoshan_2013.tif'
    gen_subtask_bbox(task_data,'xiaoshan_2013')
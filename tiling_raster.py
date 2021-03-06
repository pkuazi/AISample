import os
import gdal


import numpy as np
import os, sys
from osgeo import gdal,ogr,osr
from geotrans import GeomTrans
import pandas as pd

def tiling_image(rasterfile):
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

            band1=dataset1.ReadAsArray(xoff, yoff, BLOCK_SIZE,git)
            band2=dataset2.ReadAsArray(xoff, yoff, BLOCK_SIZE,BLOCK_SIZE)
            band3=dataset3.ReadAsArray(xoff, yoff, BLOCK_SIZE,BLOCK_SIZE)
            band1[band1 == noDataValue] = -9999
            band2[band2 == noDataValue] = -9999
            band3[band3 == noDataValue] = -9999
            
            rgb_img = bands_stack(band1,band2,band3)

            dst_file=os.path.join(seg_path,imageid+str(i)+'_'+str(j)+'.tif')
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
        
            print("start segmenting...")
            jsonfile = os.path.join(json_path, imageid+str(i)+'_'+str(j)+'.geojson')
            json_file = obia(rgb_img,dst_file,proj, gt,jsonfile)
            
            if json_file is None:
                continue
            else:
                f=open(json_file)
                geojson = json.loads(f.read())
                encode_gj= encode_json(geojson)
                dir_file=os.path.join(encode_dir, imageid+str(i)+'_'+str(j)+'.geojson')
                print(dir_file)    
                with open(dir_file, 'w') as outfile:
                    json.dump(encode_gj, outfile)
            
    bb = {}
    bb["subtaskname"]=subtask_list
    bb["minx"]=minx_list
    bb["maxy"]=maxy_list
    bb["maxx"]=maxx_list
    bb["miny"]=miny_list      

    df=pd.DataFrame(bb)
    df.to_csv('/tmp/subtask_bbox_wgs.csv')

if __name__ == '__main__':
    print('start')
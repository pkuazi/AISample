import os
from utils.resampling import resampling

ROOT_PATH = '/mnt/rsimages/lulc/AISample'
irrg_path = os.path.join(ROOT_PATH,'BANDS_IRRG')

# imageid_path = os.path.join(ROOT_PATH,'IMAGE')
# imageids_file = os.path.join(imageid_path,'imageids.csv')

RESOLUTION = 30.0

image_path = os.path.join(ROOT_PATH,'BANDS_ALL')
files = os.listdir(image_path)
file_with = {'LM1':{'IR' :'_B6', 'R' : '_B5', 'G' : '_B4'},'LM2':{'IR' :'_B6', 'R' : '_B5', 'G' : '_B4'},'LM3':{'IR' :'_B6', 'R' : '_B5', 'G' : '_B4'}, 'LT5':{'IR':'_B4', 'R' : '_B3', 'G' : '_B2'}, 'LE7':{'IR' :'_B4', 'R' : '_B3', 'G' :'_B2'}, 'LC8':{'IR' : '_B5', 'R' : '_B4', 'G' : '_B3'}}
# LT5 may also ends with B50.TIF
for file in files:
    file_path = os.path.join(image_path,file)
    bands = os.listdir(file_path)
    for bandf in bands:
        if file_with[file[0:3]]['IR'] in bandf:
            bandir = os.path.join(file_path, bandf)
        elif file_with[file[0:3]]['R'] in bandf:
            bandr = os.path.join(file_path, bandf)
        elif file_with[file[0:3]]['G'] in bandf:
            bandg = os.path.join(file_path, bandf)
                        
    outfile = os.path.join(irrg_path,file+'_IRRG.TIF')
    cmd = 'gdal_merge.py -tap -separate -o  %s -of GTiff %s %s %s'%(np.Nan,outfile, bandir, bandr, bandg)
    os.system(cmd)
    
    

                   
        

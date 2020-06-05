import os

ROOT_PATH = '/mnt/rsimages/lulc/AISample'
irrg_path = os.path.join(ROOT_PATH,'BANDS_IRRG')
image_path = os.path.join(ROOT_PATH,'BANDS_ALL')
files = os.listdir(image_path)
file_ends = {'LM1':{'IR' :'_B6.TIF', 'R' : '_B5.TIF', 'G' : '_B4.TIF'},'LM2':{'IR' :'_B6.TIF', 'R' : '_B5.TIF', 'G' : '_B4.TIF'},'LM3':{'IR' :'_B6.TIF', 'R' : '_B5.TIF', 'G' : '_B4.TIF'}, 'LT5':{'IR':'_B4.TIF', 'R' : '_B3.TIF', 'G' : '_B2.TIF'}, 'LE7':{'IR' :'_B40.TIF', 'R' : '_B30.TIF', 'G' :'_B20.TIF'}, 'LC8':{'IR' : '_B5.TIF', 'R' : '_B4.TIF', 'G' : '_B3.TIF'}}
for file in files:
    file_path = os.path.join(image_path,file)
    IR_END = file_ends[file[0:3]]['IR']
    R_END = file_ends[file[0:3]]['R']
    G_END = file_ends[file[0:3]]['G']
        
    bandir = os.path.join(file_path, file+IR_END)
    bandr = os.path.join(file_path, file+R_END)
    bandg = os.path.join(file_path, file+G_END)

    if not os.path.exists(bandir):
        bands = os.listdir(file_path)
        for bandf in bands:
            if bandf.endswith(IR_END):
                bandir = os.path.join(file_path, bandf)
            elif bandf.endswith(R_END):
                bandr = os.path.join(file_path, bandf)
            elif bandf.endswith(G_END):
                bandg = os.path.join(file_path, bandf)
                        
    outfile = os.path.join(irrg_path,file+'_IRRG.TIF')
    cmd = 'gdal_merge.py -tap -separate -o  %s -of GTiff %s %s %s'%(outfile, bandir, bandr, bandg)
    os.system(cmd)
                   


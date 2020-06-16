import os
gt='/mnt/rsimages/lulc/AISample/TILE_GT/'
irrg = '/mnt/rsimages/lulc/AISample/TILE_IRRG/'
dem = '/mnt/rsimages/lulc/AISample/TILE_DEM/'

files=os.listdir(gt)
for file in files:
#    print(file)
    start = file[0:13]
    
    ifiles = os.listdir(irrg)
    found = 0    
    for ifile in ifiles:
        if ifile.startswith(start):
            found=1
 #           print('find it!',ifile)
            continue
    if found==0:
        print('no irrg  match for ',file)
        
    dfiles = os.listdir(dem)
    found = 0
    for dfile in dfiles:
        if dfile.startswith(start):
            found=1
 #           print('find it!',ifile)
            continue
    if found==0:
        print('no dem match for ',file)

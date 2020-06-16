import os
gt='/mnt/rsimages/lulc/AISample/TILE_GT/'
irrg = '/mnt/rsimages/lulc/AISample/TILE_IRRG/'
dem = '/mnt/rsimages/lulc/AISample/TILE_DEM/'

files=os.listdir(gt)
gt_num=0
irrg_num=0
dem_num=0
for file in files:
    gt_num+=1
#    print(file)
    start = file[0:13]
    
    ifiles = os.listdir(irrg)
    found = 0    
    for ifile in ifiles:
        irrg_num+=1
        if ifile.startswith(start):
            found=1
 #           print('find it!',ifile)
            continue
    if found==0:
        print('no irrg  match for ',file)
        
    dfiles = os.listdir(dem)
    found = 0
    for dfile in dfiles:
        dem_num+=1
        if dfile.startswith(start):
            found=1
 #           print('find it!',ifile)
            continue
    if found==0:
        print('no dem match for ',file)
print('the number of  gt tiles is ', gt_num)
print('the number of  irrg tiles is ', gt_num)
print('the number of  dem tiles is ', gt_num)

import os,sys
from osgeo import gdal,ogr,osr
import json
import numpy as np

'''compute min and mix for each raster (without nodata)'''

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

def read_mm(file):
    print(file)
    ds = gdal.Open(file)
    if ds is None:
        print("Failed to open file: " + file)
        sys.exit(1)
    band = ds.GetRasterBand(1)
    noDataValue = band.GetNoDataValue()
    data = band.ReadAsArray()
    max = data[data!=noDataValue].max()
    min = data[data!=noDataValue].min()
    return min, max

def write_to_json(path, dict):
    for root, dirs, names in os.walk(path):
        for filename in names:
            if filename.endswith('.tif'):
                file_dir = os.path.join(root,filename)
                min, max = read_mm(file_dir)
                dict[file_dir]={'min':min,'max':max}
    return dict
if __name__ == '__main__':
    extreme={}
    DATA1='/mnt/bvt/环科院提供的示范数据/数据/生态参数/开化县/'
    county1 = write_to_json(DATA1, extreme)
    DATA2 = '/mnt/bvt/环科院提供的示范数据/数据/生态参数/玛多县'  
    county2 = write_to_json(DATA2, county1)
           
    j = json.dumps(county2,cls=NpEncoder)            
    fileobject = open('/tmp/minmax.json','w')
    fileobject.write(j)        
    fileobject.close()
                
            
            
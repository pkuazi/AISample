import os,json
import gjsonc

file_dir='/mnt/win/code/workspace/OBIA/seg_json'
encode_dir='/mnt/win/code/workspace/OBIA/encode_json'


if __name__ == '__main__':
#    print (json.dumps(geojson, ensure_ascii=False))
    
 #   jstr = gjsonc.encode_geojson(geojson)    
  #  print (json.dumps(jstr, ensure_ascii=False))
    
   # jjso = gjsonc.decode_geojson(jstr)
   # print (json.dumps(jjso, ensure_ascii=False))
    
   # jjso = gjsonc.trunc_geojson(geojson , 4)
   # print (json.dumps(jjso, ensure_ascii=False))
    
   # print( gjsonc.jsdecode_source() )
    
    
    
    files=[f for f in os.listdir(file_dir)]
    for n in range(len(files)):
        geojson_file=os.path.join(file_dir,files[n])
        f=open(geojson_file)
        t = json.loads(f.read())
        num_geom=len(t['features'])
        for i in range(num_geom):
            print(i)
            geom = t['features'][i]['geometry']
            geojs=gjsonc.trunc_geojson(geom,4)
            jstr = gjsonc.encode_geojson(geojs)
            t['features'][i]['geometry']=jstr
        
        dir_file=os.path.join(encode_dir, files[n])
        print(dir_file)    
        with open(dir_file, 'w') as outfile:
            json.dump(t, outfile)

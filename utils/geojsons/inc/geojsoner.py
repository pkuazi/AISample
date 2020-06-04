'''
    geojson 压缩和解压代码
'''

def encode_geojson(geojson):    
    '''
    对geojson坐标序列进行压缩unicode串
    '''
    gtype = geojson["type"].lower()
    coordinates = geojson["coordinates"]
    
    gju = GeoJsonUtils()    
    new_coordinates = []
        
    if gtype in ["MultiPolygon".lower()]: 
        for a_polygon in coordinates:
            aline = gju.EncodePoints(a_polygon[0])
            new_coordinates.append([ aline ])       

    elif gtype in ["Polygon".lower(), "MultiLineString".lower()]:   
        for a_polygon in coordinates:
            aline = gju.EncodePoints(a_polygon)
            new_coordinates.append([ aline ])  

    elif gtype in ["LineString".lower()]:      
        aline = gju.EncodePoints(coordinates)    
        new_coordinates = [aline]       
     
    geojson["coordinates"] = new_coordinates 
    return geojson 


def decode_geojson(geojson):    
    '''
    对geojson unicode串进行解压坐标序列
    '''
    gtype = geojson["type"].lower()
    coordinates = geojson["coordinates"]

    gju = GeoJsonUtils()
    new_coordinates = [] 
    
    if gtype in ["MultiPolygon".lower()]:   
        for a_polygon in coordinates: 
            a_polygon = a_polygon[0]
            aline = gju.DecodePoints(a_polygon[0], * a_polygon[1])
            new_coordinates.append([ aline ])

    elif gtype in ["Polygon".lower(), "MultiLineString".lower()]:    
        for a_polygon in coordinates: 
            a_polygon = a_polygon[0]
            aline = gju.DecodePoints(a_polygon[0], * a_polygon[1])
            new_coordinates.append(aline) 

    elif gtype in ["LineString".lower()]:    
        coordinates = coordinates[0]
        new_coordinates = gju.DecodePoints(coordinates[0], * coordinates[1])  

    geojson["coordinates"] = new_coordinates 
    return geojson 


def trunc_geojson(geojson, tail):
    '''
    对geojson坐标序列进行小数点位截取
    '''
    gtype = geojson["type"].lower()
    coordinates = geojson["coordinates"] 
    
    gju = GeoJsonUtils()
       
    new_coordinates = []
    if gtype in ["MultiPolygon".lower()]: 
        for a_polygon in coordinates: 
            aline = gju.TruncPoints(a_polygon[0], tail)
            new_coordinates.append([ aline ])

    elif gtype in ["Polygon".lower(), "MultiLineString".lower()]:           
        for a_polygon in coordinates: 
            aline = gju.TruncPoints(a_polygon, tail)
            new_coordinates.append(aline)
        
        geojson["coordinates"] = new_coordinates 

    elif gtype in ["LineString".lower()]:    
        new_coordinates = gju.TruncPoints(coordinates, tail)      

    geojson["coordinates"] = new_coordinates 
    return geojson 


def jsdecode_source():
    '''
    javascript 对geojson unicode串进行解压坐标序列的代码
    '''
    return '''
var GSON = {
    base_number : 100000.0,
    base_offset : 64,
    decode_a_line : function(coordinate, encodeOffsets) {
        var result = [];

        var prevX = encodeOffsets[0]
        var prevY = encodeOffsets[1]

        var l = coordinate.length;
        var i = 0;
        while (i < l) {
            var x = coordinate.charCodeAt(i) - this.base_offset
            var y = coordinate.charCodeAt(i + 1) - this.base_offset
            i += 2;

            x = (x >> 1) ^ (-(x & 1));
            y = (y >> 1) ^ (-(y & 1));

            x += prevX;
            y += prevY;

            prevX = x;
            prevY = y;

            result.push([ x / this.base_number, y / this.base_number ])
        }
        return result;
    }, 
    
    decode_geojson: function(geojson) {
        var gtype = geojson["type"];
        var coordinates = geojson["coordinates"];       
         
        var new_coordinates = [];        
        if( gtype == "MultiPolygon" ){ 
            for (var i = 0; i < coordinates.length; i++) {
                var a_polygon = coordinates[i];
                a_polygon = a_polygon[0];
                var aline = this.decode_a_line(a_polygon[0], a_polygon[1]);
                new_coordinates.push([ aline ])
            } 
        }else if ( gtype == "Polygon" || qtype == "MultiLineString" ){
                for (var i = 0; i < coordinates.length; i++) {
                    var a_polygon = coordinates[i];
                    a_polygon = a_polygon[0];
                    var aline = this.decode_a_line(a_polygon[0], a_polygon[1]);
                    new_coordinates.push( aline );
                } 
        } else if ( gtype == "LineString" ){ 
            var coordinates = coordinates[0];
            new_coordinates = this.decode_a_line(a_polygon[0], a_polygon[1]);
        } 
    
        geojson["coordinates"] = new_coordinates ;
        return geojson  ;
    }
};
'''


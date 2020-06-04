%pythoncode %{	
'''
	GeoJson 编码压缩模块
'''
%}

%{ 

#include "inc/gjsonc_swig.h"

%}

struct GeoJsonUtils{ 

};

%extend GeoJsonUtils {
   
   %feature("docstring", "对geojson坐标序列进行压缩unicode串") EncodePoints;
   PyObject * EncodePoints(PyObject * coords){ 
       
       return _encode_a_line( coords );
       
   } 


   %feature("docstring", "对geojson坐标序列进行小数点位截取") TruncPoints;
   PyObject * TruncPoints(PyObject * coords, int tail){ 
       
       return _trunc_a_line( coords, tail );
       
   }   
   
   %feature("docstring", "对geojson unicode串进行解压坐标序列") DecodePoints;
   PyObject * DecodePoints(PyObject * coords,  int prevX, int prevY ){   
       
       return  _decode_a_line( coords, prevX,   prevY);
       
   }
   
};


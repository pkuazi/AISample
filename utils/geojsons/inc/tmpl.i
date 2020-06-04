/* gjsonc.i */
 
%module gjsonc
 
%include "typemaps.i"

%include "exception.i" 
 
%{

%} 

%pythoncode %{	
'''
	欺骗swig编译器，加载 numpy 的 c 模板函数
'''
%}



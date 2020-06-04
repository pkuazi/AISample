#!/usr/bin/env python

import os, sys 

from setuptools import find_packages 
from platform import python_version
_packages = find_packages() 

import numpy 
from os.path import join, dirname
# from setuptools import setup

from distutils.core import setup 
from setuptools.extension import Extension 

ver = sys.version_info


include_dirs=[ '/usr/include/databox', numpy.get_include()]
 
# if ver.major == 3 :
#     if ver.minor == 7: 
#         include_dirs.append( "/usr/include/python3.7m" )           
#     elif ver.minor == 6:
#         include_dirs.append( "/usr/include/python3.6m" ) 

ext_modules = [
    Extension(
        '_gjsonc',
        sources=[  
            "./gjsonc_wrap.cxx"
        ],
        libraries=[ ],
#         extra_link_args=[ 'libcrypto.a', 'libssl.a' ],  
        extra_compile_args=[
            "-std=c++11",
            "-fPIC", '-g0', '-O3',    
            "-D__RELEASE__", 
#             "-DSWIGRUNTIME_DEBUG",
            "-D__NOLOGGER__" ],  # '-Wall', "-fPIC", "-std=c++11", '-g0', '-O3', '-Wno-cpp', 
        
         include_dirs= include_dirs
    ) 
] 

setup (name='gjsonc',
       version='1.0',
       author="SWIG Docs",
       description="""Simple gjsonc from docs""",
       ext_modules=ext_modules,
       py_modules=["gjsonc"],
       packages=_packages 
)

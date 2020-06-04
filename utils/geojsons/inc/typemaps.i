
%exception {
	try {
	$function
	} 
	catch (dbox_error & e) {
	 	PyErr_SetString(PyExc_RuntimeError, e.what());
		SWIG_fail;
	} catch(...) {
		PyErr_SetString(PyExc_RuntimeError, "Unknown exception");
		SWIG_fail;
	} 	
}; 


%typemap(out) char ** { 
	int len = 0;  
	if ($1 ) {
		len = CSLCount( $1 );
	}
	
	$result = PyList_New( len );
	
	if ($1 ) {
		for (int i=0; i < len; i++ ) {
			const char * item = CSLGetField( $1, i ); 
			PyList_SetItem( $result, i, PyBytes_FromString( item ) ); 
		}
		CSLDestroy( $1 );
	}
}

%typemap(in) char ** {
	if ($input != Py_None) { 
		if (!PySequence_Check($input) || PyUnicode_Check($input)) {
	    	PyErr_SetString(PyExc_TypeError,"Not a list or str");
	    	return NULL;
		}
	
		Py_ssize_t size = PySequence_Size($input);
		if (size != (int) size) {
			PyErr_SetString(PyExc_TypeError,"Too large sequence");
			return NULL;  
		}
		 
		for (int i = 0; i < (int) size; i++) {
			PyObject* pyObj = PySequence_GetItem($input, i);

			if (PyUnicode_Check(pyObj)) {
				PyObject* pyUTF8Str = PyUnicode_AsUTF8String(pyObj);
				char *pszStr;
				Py_ssize_t nLen;
				PyBytes_AsStringAndSize(pyUTF8Str, &pszStr, &nLen);
				$1  = CSLAddString($1 , pszStr);
				Py_XDECREF(pyUTF8Str);

			} else if (PyBytes_Check(pyObj))
				$1  = CSLAddString($1 , PyBytes_AsString(pyObj));

			else {
				Py_DECREF(pyObj);
				PyErr_SetString(PyExc_TypeError,"Sequence must be string");
				return NULL; 
			}
			Py_DECREF(pyObj);
		} 
	}
}

// This cleans up the char ** array we malloc'd before the function call
%typemap(freearg) char ** {
	if ( $1) {
		CSLDestroy( $1 );
	}
}


#ifndef BASE_H_
#define BASE_H_

#ifndef __RELEASE__
#include <python3.7m/Python.h>
#else
#include <Python.h>
#endif

//#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
//#include <numpy/arrayobject.h>

/* Compatibility macros for Python 3 */
#if PY_VERSION_HEX >= 0x03000000

#define PyInt_Check(x) PyLong_Check(x)
#define PyInt_AsLong(x) PyLong_AsLong(x)
#define PyInt_FromLong(x) PyLong_FromLong(x)
#define PyInt_FromSize_t(x) PyLong_FromSize_t(x)

#define PyString_Check(name) PyBytes_Check(name)
#define PyString_FromString(x) PyUnicode_FromString(x)
#define PyString_Format(fmt, args)  PyUnicode_Format(fmt, args)

#define PyString_AsString(str) PyBytes_AsString(str)
#define PyString_Size(str) PyBytes_Size(str)

#define PyString_InternFromString(key) PyUnicode_InternFromString(key)
#define Py_TPFLAGS_HAVE_CLASS Py_TPFLAGS_BASETYPE

#define PyString_AS_STRING(x) PyUnicode_AS_STRING(x)
#define _PyLong_FromSsize_t(x) PyLong_FromSsize_t(x)

//PyUnicode_GetLength

#endif

template<typename _T>

static _T py_cast_as_number(PyObject *a) {
	if (PyLong_CheckExact(a)) {
		return (_T) PyLong_AsDouble(a);

	} else if (PyFloat_CheckExact(a)) {
		return (_T) PyFloat_AsDouble(a);

#if PY_VERSION_HEX < 0x03000000
	} else if (PyInt_CheckExact(a)) {
		return (_T) PyInt_AsLong(a);
#endif

	}
	return 0;
}

class dbox_error: public std::logic_error {
public:
	dbox_error(const std::string &__arg) :
			std::logic_error(__arg) {
	}
};

static void throw_dbox(const char *message) {
	if (message) {
		throw dbox_error(message);
	} else {
		throw dbox_error("nothing");
	}
}

static void throw_dbox(const char *message, const std::string &filename) {
	if (message) {
		throw dbox_error(std::string(message) + ": " + filename);
	} else {
		throw dbox_error(filename);
	}
}

#endif

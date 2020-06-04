#ifndef GJSONC_SWIG_H_
#define GJSONC_SWIG_H_

#include "base.h"

#include <stddef.h>
#include <cctype>
#include <cmath>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <utility>

namespace std {

template<typename _Tp>
class auto_free_ptr {
private:
	_Tp *buffer_ { NULL };
	size_t bufsize_ { 0 };
public:

	auto_free_ptr() {
	}

	auto_free_ptr(_Tp *buffer) :
			buffer_(buffer) {
	}

	~auto_free_ptr() {
		clear();
	}

	_Tp* resize(const size_t newsize) {
		if (newsize == 0) {
			return NULL;
		}

		if (this->buffer_ != NULL) {
			if (newsize <= this->bufsize_) {
				return this->buffer_;
			}
			delete this->buffer_;
		}

		try {
			this->buffer_ = new _Tp[newsize];
			this->bufsize_ = newsize;
			return this->buffer_;
		} catch (std::exception &e) {
			return NULL;
		}
	}

	void clear() {
		if (this->buffer_ == NULL) {
			return;
		}
		delete this->buffer_;
		this->buffer_ = NULL;
		this->bufsize_ = 0;
	}

	_Tp* operator ->() {
		return this->buffer_;
	}

	_Tp at(int idx) {
		return this->buffer_[idx];
	}

	_Tp* get() {
		return this->buffer_;
	}

	/**
	 * 重置内部指针，返回内部指针的指针地址
	 * 方便 caller 设置地址，caller不需要再次 free，内部自动管理了 buffer
	 */
	_Tp** addr() {
		clear();
		return &this->buffer_;
	}

	void reset(_Tp *newbuffer, size_t newsize) {
		clear();
		this->buffer_ = newbuffer;
		this->bufsize_ = newsize;
	}

	/**
	 * 重置内部 buffer 为 NULL，且返回原 buffer 地址，内部数据还在
	 * 内部分配的 buffer 由 caller 去 free，如果不free，会出现内存泄露问题
	 */
	_Tp* reset() {
		_Tp *old_buffer = this->buffer_;
		this->buffer_ = NULL;
		this->bufsize_ = 0;
		return old_buffer;
	}
};

}
;

//#include <databox/stringutils.hpp>

struct GeoJsonUtils {

};

#define _base_number 100000.0
#define _base_offset 64

#define  quantize(val) static_cast<int>(ceil(val * _base_number))

#define WCharFromCode(val)  (wchar_t) (val & 0x10FFFF)

namespace geomseqs {

//static std::string str_lower(const std::string &src) {
//	std::string data = src;
//	std::transform(data.begin(), data.end(), data.begin(), ::tolower);
//	return data;
//}

static wchar_t encode(double val, int prev) {
	int val0 = quantize(val)- prev;
	val0 = ((val0 << 1) ^ (val0 >> 21)) + _base_offset;
	if (val == 8232) {val = 9231;}
	return WCharFromCode(val0);
}

static int EncodeLine(const std::vector<double> &v_coords, std::wstring &result, int &encodeOffsetX,
		int &encodeOffsetY) {
	int len = v_coords.size() / 2;
	if (len == 0) {
		return 0;
	}

	int wlen = len * 2;
	wchar_t *tbuffer = new wchar_t[wlen];

	int prevX = encodeOffsetX = quantize(v_coords[0]);
	int prevY = encodeOffsetY = quantize(v_coords[1]);

	for (int i = 0; i < len; i++) {
		double xval = v_coords[i * 2];
		tbuffer[i * 2] = encode(xval, prevX);

		double yval = v_coords[i * 2 + 1];
		tbuffer[i * 2 + 1] = encode(yval, prevY);

		prevX = quantize(xval);
		prevY = quantize(yval);
	}

	result.assign(tbuffer, wlen);
	delete tbuffer;

	return len;
}

static int DecodeLine(const std::wstring &lines, int encodeOffsetX, int encodeOffsetY, std::vector<double> &v_coords) {
	int prevX = encodeOffsetX;
	int prevY = encodeOffsetY;

	int len = lines.size();
	int i = 0;

	while (i < len) {
		int x = (int) lines[i++] - _base_offset;
		int y = (int) lines[i++] - _base_offset;

		//		x = ord(coordinate[i]) - _base_offset
		//		y = ord(coordinate[i + 1]) - _base_offset
		//		i += 2

		x = (x >> 1) ^ (-(x & 1));
		y = (y >> 1) ^ (-(y & 1));

		x += prevX;
		y += prevY;

		prevX = x;
		prevY = y;

		double dx = x / _base_number;
		double dy = y / _base_number;

		v_coords.push_back(dx);
		v_coords.push_back(dy);
	}
	return len;
}

}
;

PyObject* _decode_a_line(PyObject *coords, int prevX, int prevY) {
	if (!PyUnicode_Check(coords)) {
		throw_dbox("Expecting a unicode str");
		return 0;
	}

	ssize_t usize = PyUnicode_GetLength(coords); // PyUnicode_GetSize
	wchar_t *wpsbuf = new wchar_t[usize + 1];

	std::auto_free_ptr<wchar_t> wpsbuf_g(wpsbuf);

	Py_ssize_t rsize = PyUnicode_AsWideChar(coords, wpsbuf, usize);
	if (rsize < 0) {
		return PyList_New(0);
	}

	std::wstring ws(wpsbuf, rsize);
	std::vector<double> v_coords;

	geomseqs::DecodeLine(ws, prevX, prevY, v_coords);

	int len = v_coords.size() / 2;

	PyObject *result = PyList_New(len);

	for (int i = 0; i < len; i++) {
		double x = v_coords[i * 2 + 0];
		double y = v_coords[i * 2 + 1];

		PyObject *pt = PyList_New(2);

		PyList_SET_ITEM(pt, 0, PyFloat_FromDouble(x));
		PyList_SET_ITEM(pt, 1, PyFloat_FromDouble(y));

		PyList_SET_ITEM(result, i, pt);
	}
	return result;
}

PyObject* _trunc_a_line(PyObject *coords, int tail) {
	if (!PyList_CheckExact(coords)) {
		PyErr_SetString(PyExc_TypeError, "Expecting a list");
		return 0;
	}

	if (tail < 0 || tail > 8) {
		PyErr_SetString(PyExc_TypeError, "Invalid value range: 0-7");
		return 0;
	}

	int coords_len = PyList_GET_SIZE(coords);
	PyObject *result = PyList_New(coords_len);

	for (int i = 0; i < coords_len; i++) {
		PyObject *coord = PyList_GET_ITEM(coords, i);
		if (!PyList_CheckExact(coord)) {
			PyErr_SetString(PyExc_TypeError, "Expecting a list of list[2]");
			return 0;
		}

		if (PyList_GET_SIZE(coord) != 2) {
			PyErr_SetString(PyExc_TypeError, "Expecting a list of list[2]");
			return 0;
		}

		double x = py_cast_as_number<double>(PyList_GET_ITEM(coord, 0));
		double y = py_cast_as_number<double>(PyList_GET_ITEM(coord, 1));

		int tail_v = 1;
		for (int k = 0; k < tail; k++) {
			tail_v *= 10;
		}

		x = ((int) (x * tail_v)) / (double) tail_v;
		y = ((int) (y * tail_v)) / (double) tail_v;

		PyObject *pt = PyList_New(2);

		PyList_SET_ITEM(pt, 0, PyFloat_FromDouble(x));
		PyList_SET_ITEM(pt, 1, PyFloat_FromDouble(y));

		PyList_SET_ITEM(result, i, pt);
	}
	return result;
}

PyObject* _encode_a_line(PyObject *coords) {
	if (!PyList_CheckExact(coords)) {
		PyErr_SetString(PyExc_TypeError, "Expecting a list");
		return 0;
	}

	int coords_len = PyList_GET_SIZE(coords);

	std::vector<double> v_coords;

	for (int i = 0; i < coords_len; i++) {
		PyObject *coord = PyList_GET_ITEM(coords, i);

		if (!PyList_CheckExact(coord)) {
			PyErr_SetString(PyExc_TypeError, "Expecting a list of list[2]");
			return 0;
		}

		if (PyList_GET_SIZE(coord) != 2) {
			PyErr_SetString(PyExc_TypeError, "Expecting a list of list[2]");
			return 0;
		}

		v_coords.push_back(py_cast_as_number<double>(PyList_GET_ITEM(coord, 0)));
		v_coords.push_back(py_cast_as_number<double>(PyList_GET_ITEM(coord, 1)));
	}

	std::wstring ws;
	int prevX, prevY;

	int r = geomseqs::EncodeLine(v_coords, ws, prevX, prevY);

	if (r == 0) {
		return PyList_New(0);
	}

	PyObject *result = PyList_New(2);

	PyList_SET_ITEM(result, 0, PyUnicode_FromWideChar(ws.c_str(), ws.size()));

	PyObject *offset = PyList_New(2);
	PyList_SET_ITEM(offset, 0, PyInt_FromLong(prevX));
	PyList_SET_ITEM(offset, 1, PyInt_FromLong(prevY));

	PyList_SET_ITEM(result, 1, offset);
	return result;
}

#endif

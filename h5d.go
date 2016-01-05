package hdf5

// #include "hdf5.h"
// #include <stdlib.h>
// #include <string.h>
import "C"

import (
	"fmt"

	"reflect"
	"runtime"
	"unsafe"
)

type Dataset Identifier

func newDataset(id C.hid_t) *Dataset {
	d := &Dataset{id: id}
	runtime.SetFinalizer(d, (*Dataset).finalizer)
	return d
}

func createDataset(id C.hid_t, name string, dtype *Datatype, dspace *Dataspace, dcpl *PropList) (*Dataset, error) {
	dtype, err := dtype.Copy() // For safety
	if err != nil {
		return nil, err
	}
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))
	hid := C.H5Dcreate2(id, c_name, dtype.id, dspace.id, P_DEFAULT.id, dcpl.id, P_DEFAULT.id)
	if err := checkID(hid); err != nil {
		return nil, err
	}
	return newDataset(hid), nil
}

func (s *Dataset) finalizer() {
	if err := s.Close(); err != nil {
		panic(fmt.Errorf("error closing dset: %s", err))
	}
}

// Close releases and terminates access to a dataset.
func (s *Dataset) Close() error {
	if s.id == 0 {
		return nil
	}
	err := h5err(C.H5Dclose(s.id))
	s.id = 0
	return err
}

// Space returns an identifier for a copy of the dataspace for a dataset.
func (s *Dataset) Space() *Dataspace {
	hid := C.H5Dget_space(s.id)
	if int(hid) > 0 {
		return newDataspace(hid)
	}
	return nil
}

// ReadSubset reads a subset of raw data from a dataset into a buffer.
func (s *Dataset) ReadSubset(data interface{}, memspace, filespace *Dataspace) error {

	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("Attribute: Read (non-pointer %v )", v.Kind())
	}

	var addr uintptr
	var err error
	var typ *Datatype

	switch v.Elem().Kind() {

	case reflect.Array:
		typ, err = NewDataTypeFromType(v.Type().Elem().Elem())
		addr = v.Elem().UnsafeAddr()

	case reflect.Slice:
		typ, err = NewDataTypeFromType(v.Type().Elem().Elem())
		addr = (*reflect.SliceHeader)(unsafe.Pointer(v.Elem().UnsafeAddr())).Data

	case reflect.String:
		typ, err = NewDataTypeFromType(v.Type().Elem())
		addr = (*reflect.StringHeader)(unsafe.Pointer(v.Elem().UnsafeAddr())).Data

	case reflect.Ptr:
		return s.ReadSubset(reflect.Indirect(v).Interface(), memspace, filespace)

	default:
		typ, err = NewDataTypeFromType(v.Type().Elem())
		addr = v.Elem().UnsafeAddr()
	}

	defer typ.Close()
	if err != nil {
		return err
	}

	var f_id, m_id C.hid_t = 0, 0
	if memspace != nil {
		m_id = memspace.id
	}
	if filespace != nil {
		f_id = filespace.id
	}

	return h5err(C.H5Dread(s.id, typ.id, m_id, f_id, 0, unsafe.Pointer(addr)))
}

// Read reads raw data from a dataset into a buffer.
func (s *Dataset) Read(data interface{}) error {
	return s.ReadSubset(data, nil, nil)
}

// WriteSubset writes a subset of raw data from a buffer to a dataset.
func (s *Dataset) WriteSubset(data interface{}, memspace, filespace *Dataspace) error {
	dtype, err := s.Datatype()
	defer dtype.Close()
	if err != nil {
		return err
	}

	addr := unsafe.Pointer(nil)
	v := reflect.Indirect(reflect.ValueOf(data))

	switch v.Kind() {

	case reflect.Array:
		addr = unsafe.Pointer(v.UnsafeAddr())

	case reflect.Slice:
		slice := (*reflect.SliceHeader)(unsafe.Pointer(v.UnsafeAddr()))
		addr = unsafe.Pointer(slice.Data)

	case reflect.String:
		str := (*reflect.StringHeader)(unsafe.Pointer(v.UnsafeAddr()))
		addr = unsafe.Pointer(str.Data)

	case reflect.Ptr:
		addr = unsafe.Pointer(v.Pointer())

	default:
		addr = unsafe.Pointer(v.UnsafeAddr())
	}

	var filespace_id, memspace_id C.hid_t = 0, 0
	if memspace != nil {
		memspace_id = memspace.id
	}
	if filespace != nil {
		filespace_id = filespace.id
	}

	return h5err(C.H5Dwrite(s.id, dtype.id, memspace_id, filespace_id, 0, addr))
}

// Write writes raw data from a buffer to a dataset.
func (s *Dataset) Write(data interface{}) error {
	return s.WriteSubset(data, nil, nil)
}

// Creates a new attribute at this location.
func (s *Dataset) CreateAttribute(name string, dtype *Datatype, dspace *Dataspace) (*Attribute, error) {
	return createAttribute(s.id, name, dtype, dspace, P_DEFAULT)
}

// Creates a new attribute at this location.
func (s *Dataset) CreateAttributeWith(name string, dtype *Datatype, dspace *Dataspace, acpl *PropList) (*Attribute, error) {
	return createAttribute(s.id, name, dtype, dspace, acpl)
}

// Opens an existing attribute.
func (s *Dataset) OpenAttribute(name string) (*Attribute, error) {
	return openAttribute(s.id, name)
}

// Datatype returns the HDF5 Datatype of the Dataset
func (s *Dataset) Datatype() (*Datatype, error) {
	dtype_id := C.H5Dget_type(s.id)
	if dtype_id < 0 {
		return nil, fmt.Errorf("couldn't open Datatype from Dataset %q", s.Name())
	}
	return newDatatype(dtype_id), nil
}

func (s *Dataset) Name() string {
	return ((*Identifier)(s)).Name()
}

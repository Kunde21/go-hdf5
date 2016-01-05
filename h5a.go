package hdf5

// #include "hdf5.h"
// #include <stdlib.h>
// #include <string.h>
import "C"

import (
	"fmt"
	"strings"

	"reflect"
	"runtime"
	"unsafe"
)

type Attribute Identifier

func newAttribute(id C.hid_t) *Attribute {
	d := &Attribute{id: id}
	runtime.SetFinalizer(d, (*Attribute).finalizer)
	return d
}

func createAttribute(id C.hid_t, name string, dtype *Datatype, dspace *Dataspace, acpl *PropList) (*Attribute, error) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))
	hid := C.H5Acreate2(id, c_name, dtype.id, dspace.id, acpl.id, P_DEFAULT.id)
	if err := checkID(hid); err != nil {
		return nil, err
	}
	return newAttribute(hid), nil
}

func openAttribute(id C.hid_t, name string) (*Attribute, error) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))

	hid := C.H5Aopen(id, c_name, P_DEFAULT.id)
	if err := checkID(hid); err != nil {
		return nil, err
	}
	return newAttribute(hid), nil
}

func getNumAttrs(id Identifier) int {
	return int(C.H5Aget_num_attrs(id.id))
}

// Helper function to be implemented on other Attribute-enabled types (File, Group, Dataset, etc)
func readAttr(id Identifier, name string, data interface{}) error {
	attr, err := openAttribute(id.id, name)
	if err != nil {
		return fmt.Errorf("Error %v opening Attribute, may not exist or name is incorrect", err)
	}
	defer attr.Close()
	return attr.Read(data)
}

func GetAttribute(id Identifier, name string) (*Attribute, error) {
	return openAttribute(id.id, name)
}

func (s *Attribute) finalizer() {
	if err := s.Close(); err != nil {
		panic(fmt.Errorf("error closing attr: %s", err))
	}
}

func (s *Attribute) Id() int {
	return int(s.id)
}

// Access the type of an attribute
func (s *Attribute) GetType() *Datatype {
	return newDatatype(C.H5Aget_type(s.id))
}

// Close releases and terminates access to an attribute.
func (s *Attribute) Close() error {
	if s.id == 0 {
		return nil
	}
	err := h5err(C.H5Aclose(s.id))
	s.id = 0
	return err
}

// Space returns an identifier for a copy of the dataspace for a attribute.
func (s *Attribute) Space() *Dataspace {
	hid := C.H5Aget_space(s.id)
	if int(hid) > 0 {
		return newDataspace(hid)
	}
	return nil
}

// Read reads raw data from a attribute into a buffer.
func (s *Attribute) Read(data interface{}) error {
	v := reflect.ValueOf(data)

	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("Attribute: Read (non-pointer %v )", v.Kind())
	}

	var addr uintptr
	var err error
	var typ *Datatype

	switch v.Elem().Kind() {

	case reflect.Array:
		if v.Elem().Len() == 0 {
			return nil
		}
		typ, err = NewDataTypeFromType(v.Type().Elem().Elem())
		addr = v.Elem().UnsafeAddr()

	case reflect.String: //Special Case read in order to trim null chars
		typ, err = NewDataTypeFromType(v.Type().Elem())

		var buf string
		if ln := int(C.H5Aget_storage_size(s.id)); ln <= v.Elem().Len() {
			buf = v.Elem().Slice(0, ln).Interface().(string)
		} else {
			buf = strings.Repeat("\x00", ln)
		}
		rc := h5err(C.H5Aread(s.id, typ.id, unsafe.Pointer(&buf)))
		if rc != nil {
			return rc
		}
		v.Elem().SetString(strings.Trim(buf, "\x00"))
		return nil
	case reflect.Slice:
		typ, err = NewDataTypeFromType(v.Type().Elem().Elem())
		if ln := int(C.H5Aget_storage_size(s.id)) / int(typ.Size()); ln <= v.Elem().Cap() {
			v.Elem().SetLen(ln)
		} else {
			reflect.Indirect(v).Set(reflect.MakeSlice(v.Elem().Type(), ln, ln))
		}
		addr = ((*reflect.SliceHeader)(unsafe.Pointer(v.Elem().UnsafeAddr()))).Data

	case reflect.Ptr:
		return s.Read(reflect.Indirect(v).Interface())

	default:
		typ, err = NewDataTypeFromType(v.Type().Elem())
		addr = v.Elem().UnsafeAddr()
	}

	defer typ.Close()
	if err != nil {
		return err
	}

	return h5err(C.H5Aread(s.id, typ.id, unsafe.Pointer(addr)))
}

// Write writes raw data from a buffer to an attribute.
func (s *Attribute) Write(data interface{}) error {

	v := reflect.ValueOf(data)

	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("Attribute: Write (non-pointer %v )", v.Type())
	}

	var addr uintptr

	switch v.Elem().Kind() {

	case reflect.Array:
		addr = v.Elem().UnsafeAddr()

	case reflect.String:
		dtype, err := NewDataTypeFromType(v.Elem().Type())
		str := v.Elem().Interface().(string)
		if err != nil {
			return fmt.Errorf("Datatype error: %v", err)
		}
		return h5err(C.H5Awrite(s.id, dtype.id, unsafe.Pointer(&str)))

	case reflect.Slice:
		addr = ((*reflect.SliceHeader)(unsafe.Pointer(v.Elem().UnsafeAddr()))).Data

	case reflect.Ptr:
		return s.Write(reflect.Indirect(v).Interface())

	default:
		addr = v.Elem().UnsafeAddr()
	}

	dtype, err := NewDataTypeFromType(v.Elem().Type())
	if err != nil {
		return fmt.Errorf("Datatype error: %v", err)
	}

	return h5err(C.H5Awrite(s.id, dtype.id, unsafe.Pointer(addr)))
}

// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/dbsteward/dbsteward/lib/format/pgsql8/live (interfaces: Connection)

// Package live is a generated GoMock package.
package live

import (
	gomock "github.com/golang/mock/gomock"
	v4 "github.com/jackc/pgx/v4"
	reflect "reflect"
)

// MockConnection is a mock of Connection interface
type MockConnection struct {
	ctrl     *gomock.Controller
	recorder *MockConnectionMockRecorder
}

// MockConnectionMockRecorder is the mock recorder for MockConnection
type MockConnectionMockRecorder struct {
	mock *MockConnection
}

// NewMockConnection creates a new mock instance
func NewMockConnection(ctrl *gomock.Controller) *MockConnection {
	mock := &MockConnection{ctrl: ctrl}
	mock.recorder = &MockConnectionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockConnection) EXPECT() *MockConnectionMockRecorder {
	return m.recorder
}

// Disconnect mocks base method
func (m *MockConnection) Disconnect() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Disconnect")
}

// Disconnect indicates an expected call of Disconnect
func (mr *MockConnectionMockRecorder) Disconnect() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Disconnect", reflect.TypeOf((*MockConnection)(nil).Disconnect))
}

// Query mocks base method
func (m *MockConnection) Query(arg0 string, arg1 ...interface{}) (v4.Rows, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Query", varargs...)
	ret0, _ := ret[0].(v4.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Query indicates an expected call of Query
func (mr *MockConnectionMockRecorder) Query(arg0 interface{}, arg1 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Query", reflect.TypeOf((*MockConnection)(nil).Query), varargs...)
}

// QueryMap mocks base method
func (m *MockConnection) QueryMap(arg0 string, arg1 ...interface{}) (StringMapList, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryMap", varargs...)
	ret0, _ := ret[0].(StringMapList)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryMap indicates an expected call of QueryMap
func (mr *MockConnectionMockRecorder) QueryMap(arg0 interface{}, arg1 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryMap", reflect.TypeOf((*MockConnection)(nil).QueryMap), varargs...)
}

// QueryRow mocks base method
func (m *MockConnection) QueryRow(arg0 string, arg1 ...interface{}) v4.Row {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryRow", varargs...)
	ret0, _ := ret[0].(v4.Row)
	return ret0
}

// QueryRow indicates an expected call of QueryRow
func (mr *MockConnectionMockRecorder) QueryRow(arg0 interface{}, arg1 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryRow", reflect.TypeOf((*MockConnection)(nil).QueryRow), varargs...)
}

// QueryVal mocks base method
func (m *MockConnection) QueryVal(arg0 interface{}, arg1 string, arg2 ...interface{}) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryVal", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// QueryVal indicates an expected call of QueryVal
func (mr *MockConnectionMockRecorder) QueryVal(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryVal", reflect.TypeOf((*MockConnection)(nil).QueryVal), varargs...)
}

// Version mocks base method
func (m *MockConnection) Version() (VersionNum, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Version")
	ret0, _ := ret[0].(VersionNum)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Version indicates an expected call of Version
func (mr *MockConnectionMockRecorder) Version() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Version", reflect.TypeOf((*MockConnection)(nil).Version))
}

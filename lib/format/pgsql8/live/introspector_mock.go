// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/dbsteward/dbsteward/lib/format/pgsql8/live (interfaces: Introspector)

// Package live is a generated GoMock package.
package live

import (
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// MockIntrospector is a mock of Introspector interface
type MockIntrospector struct {
	ctrl     *gomock.Controller
	recorder *MockIntrospectorMockRecorder
}

// MockIntrospectorMockRecorder is the mock recorder for MockIntrospector
type MockIntrospectorMockRecorder struct {
	mock *MockIntrospector
}

// NewMockIntrospector creates a new mock instance
func NewMockIntrospector(ctrl *gomock.Controller) *MockIntrospector {
	mock := &MockIntrospector{ctrl: ctrl}
	mock.recorder = &MockIntrospectorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockIntrospector) EXPECT() *MockIntrospectorMockRecorder {
	return m.recorder
}

// GetColumns mocks base method
func (m *MockIntrospector) GetColumns(arg0, arg1 string) ([]ColumnEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetColumns", arg0, arg1)
	ret0, _ := ret[0].([]ColumnEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetColumns indicates an expected call of GetColumns
func (mr *MockIntrospectorMockRecorder) GetColumns(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetColumns", reflect.TypeOf((*MockIntrospector)(nil).GetColumns), arg0, arg1)
}

// GetConstraints mocks base method
func (m *MockIntrospector) GetConstraints() ([]ConstraintEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetConstraints")
	ret0, _ := ret[0].([]ConstraintEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetConstraints indicates an expected call of GetConstraints
func (mr *MockIntrospectorMockRecorder) GetConstraints() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetConstraints", reflect.TypeOf((*MockIntrospector)(nil).GetConstraints))
}

// GetForeignKeys mocks base method
func (m *MockIntrospector) GetForeignKeys() ([]ForeignKeyEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetForeignKeys")
	ret0, _ := ret[0].([]ForeignKeyEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetForeignKeys indicates an expected call of GetForeignKeys
func (mr *MockIntrospectorMockRecorder) GetForeignKeys() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetForeignKeys", reflect.TypeOf((*MockIntrospector)(nil).GetForeignKeys))
}

// GetFunctionArgs mocks base method
func (m *MockIntrospector) GetFunctionArgs(arg0 Oid) ([]FunctionArgEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFunctionArgs", arg0)
	ret0, _ := ret[0].([]FunctionArgEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetFunctionArgs indicates an expected call of GetFunctionArgs
func (mr *MockIntrospectorMockRecorder) GetFunctionArgs(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFunctionArgs", reflect.TypeOf((*MockIntrospector)(nil).GetFunctionArgs), arg0)
}

// GetFunctions mocks base method
func (m *MockIntrospector) GetFunctions() ([]FunctionEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFunctions")
	ret0, _ := ret[0].([]FunctionEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetFunctions indicates an expected call of GetFunctions
func (mr *MockIntrospectorMockRecorder) GetFunctions() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFunctions", reflect.TypeOf((*MockIntrospector)(nil).GetFunctions))
}

// GetIndexes mocks base method
func (m *MockIntrospector) GetIndexes(arg0, arg1 string) ([]IndexEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetIndexes", arg0, arg1)
	ret0, _ := ret[0].([]IndexEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetIndexes indicates an expected call of GetIndexes
func (mr *MockIntrospectorMockRecorder) GetIndexes(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetIndexes", reflect.TypeOf((*MockIntrospector)(nil).GetIndexes), arg0, arg1)
}

// GetSchemaOwner mocks base method
func (m *MockIntrospector) GetSchemaOwner(arg0 string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSchemaOwner", arg0)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSchemaOwner indicates an expected call of GetSchemaOwner
func (mr *MockIntrospectorMockRecorder) GetSchemaOwner(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSchemaOwner", reflect.TypeOf((*MockIntrospector)(nil).GetSchemaOwner), arg0)
}

// GetSequencePerms mocks base method
func (m *MockIntrospector) GetSequencePerms(arg0 string) ([]SequencePermEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSequencePerms", arg0)
	ret0, _ := ret[0].([]SequencePermEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSequencePerms indicates an expected call of GetSequencePerms
func (mr *MockIntrospectorMockRecorder) GetSequencePerms(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSequencePerms", reflect.TypeOf((*MockIntrospector)(nil).GetSequencePerms), arg0)
}

// GetSequenceRelList mocks base method
func (m *MockIntrospector) GetSequenceRelList(arg0 string, arg1 []string) ([]SequenceRelEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSequenceRelList", arg0, arg1)
	ret0, _ := ret[0].([]SequenceRelEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSequenceRelList indicates an expected call of GetSequenceRelList
func (mr *MockIntrospectorMockRecorder) GetSequenceRelList(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSequenceRelList", reflect.TypeOf((*MockIntrospector)(nil).GetSequenceRelList), arg0, arg1)
}

// GetSequencesForRel mocks base method
func (m *MockIntrospector) GetSequencesForRel(arg0, arg1 string) ([]SequenceEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSequencesForRel", arg0, arg1)
	ret0, _ := ret[0].([]SequenceEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSequencesForRel indicates an expected call of GetSequencesForRel
func (mr *MockIntrospectorMockRecorder) GetSequencesForRel(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSequencesForRel", reflect.TypeOf((*MockIntrospector)(nil).GetSequencesForRel), arg0, arg1)
}

// GetTableList mocks base method
func (m *MockIntrospector) GetTableList() ([]TableEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTableList")
	ret0, _ := ret[0].([]TableEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTableList indicates an expected call of GetTableList
func (mr *MockIntrospectorMockRecorder) GetTableList() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTableList", reflect.TypeOf((*MockIntrospector)(nil).GetTableList))
}

// GetTablePerms mocks base method
func (m *MockIntrospector) GetTablePerms() ([]TablePermEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTablePerms")
	ret0, _ := ret[0].([]TablePermEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTablePerms indicates an expected call of GetTablePerms
func (mr *MockIntrospectorMockRecorder) GetTablePerms() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTablePerms", reflect.TypeOf((*MockIntrospector)(nil).GetTablePerms))
}

// GetTableStorageOptions mocks base method
func (m *MockIntrospector) GetTableStorageOptions(arg0, arg1 string) (map[string]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTableStorageOptions", arg0, arg1)
	ret0, _ := ret[0].(map[string]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTableStorageOptions indicates an expected call of GetTableStorageOptions
func (mr *MockIntrospectorMockRecorder) GetTableStorageOptions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTableStorageOptions", reflect.TypeOf((*MockIntrospector)(nil).GetTableStorageOptions), arg0, arg1)
}

// GetTriggers mocks base method
func (m *MockIntrospector) GetTriggers() ([]TriggerEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTriggers")
	ret0, _ := ret[0].([]TriggerEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTriggers indicates an expected call of GetTriggers
func (mr *MockIntrospectorMockRecorder) GetTriggers() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTriggers", reflect.TypeOf((*MockIntrospector)(nil).GetTriggers))
}

// GetViews mocks base method
func (m *MockIntrospector) GetViews() ([]ViewEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetViews")
	ret0, _ := ret[0].([]ViewEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetViews indicates an expected call of GetViews
func (mr *MockIntrospectorMockRecorder) GetViews() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetViews", reflect.TypeOf((*MockIntrospector)(nil).GetViews))
}

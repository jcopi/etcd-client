package etcdclient

import (
	"encoding/base64"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type EtcdValue interface {
	ToString() string
	FromString(string) error
	IsGet() bool
	IsSet() bool
	IsDelete() bool
}

type EtcdTime time.Time
type EtcdUuid string
type EtcdString string
type EtcdInt int64
type EtcdUint uint64
type EtcdBool bool
type EtcdBytes []byte

var (
	getTimeOp    EtcdTime
	deleteTimeOp EtcdTime

	getUUIDOp    EtcdUuid
	deleteUUIDOp EtcdUuid

	getStringOp    EtcdString
	deleteStringOp EtcdString

	getIntOp    EtcdInt
	deleteIntOp EtcdInt

	getUintOp    EtcdUint
	deleteUintOp EtcdUint

	getBoolOp    EtcdBool
	deleteBoolOp EtcdBool

	getBytesOp    EtcdBytes
	deleteBytesOp EtcdBytes

	getSliceOp    []EtcdValue
	deleteSliceOp []EtcdValue
)

func (e *EtcdTime) ToString() string {
	if e == nil {
		return ""
	}
	return time.Time(*e).Format(time.RFC3339)
}
func (e *EtcdTime) FromString(value string) error {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return err
	}
	*e = EtcdTime(t)
	return nil
}
func (e *EtcdTime) IsGet() bool {
	return e == &getTimeOp
}
func (e *EtcdTime) IsSet() bool {
	return e != &deleteTimeOp && e != nil
}
func (e *EtcdTime) IsDelete() bool {
	return e == &deleteTimeOp
}

func (e *EtcdUuid) ToString() string {
	if e == nil {
		return ""
	}
	return string(*e)
}
func (e *EtcdUuid) FromString(value string) error {
	// Checks should be put in place to verify this is an actual UUID
	*e = EtcdUuid(value)
	return nil
}
func (e *EtcdUuid) IsGet() bool {
	return e == &getUUIDOp
}
func (e *EtcdUuid) IsSet() bool {
	return e != &deleteUUIDOp && e != nil
}
func (e *EtcdUuid) IsDelete() bool {
	return e == &deleteUUIDOp
}

func (e *EtcdString) ToString() string {
	if e == nil {
		return ""
	}
	return string(*e)
}
func (e *EtcdString) FromString(value string) error {
	*e = EtcdString(value)
	return nil
}
func (e *EtcdString) IsGet() bool {
	return e == &getStringOp
}
func (e *EtcdString) IsSet() bool {
	return e != &deleteStringOp && e != nil
}
func (e *EtcdString) IsDelete() bool {
	return e == &deleteStringOp
}

func (e *EtcdInt) ToString() string {
	return strconv.FormatInt(int64(*e), 10)
}
func (e *EtcdInt) FromString(value string) error {
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return err
	}
	*e = EtcdInt(i)
	return nil
}
func (e *EtcdInt) IsGet() bool {
	return e == &getIntOp
}
func (e *EtcdInt) IsSet() bool {
	return e != &deleteIntOp && e != nil
}
func (e *EtcdInt) IsDelete() bool {
	return e == &deleteIntOp
}
func (e *EtcdUint) ToString() string {
	if e == nil {
		return ""
	}
	return strconv.FormatUint(uint64(*e), 10)
}
func (e *EtcdUint) FromString(value string) error {
	u, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return err
	}
	*e = EtcdUint(u)
	return nil
}
func (e *EtcdUint) IsGet() bool {
	return e == &getUintOp
}
func (e *EtcdUint) IsSet() bool {
	return e != &deleteUintOp && e != nil
}
func (e *EtcdUint) IsDelete() bool {
	return e == &deleteUintOp
}
func (e *EtcdBool) ToString() string {
	return strconv.FormatBool(bool(*e))
}
func (e *EtcdBool) FromString(value string) error {
	b, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	*e = EtcdBool(b)
	return nil
}
func (e *EtcdBool) IsGet() bool {
	return e == &getBoolOp
}
func (e *EtcdBool) IsSet() bool {
	return e != &deleteBoolOp && e != nil
}
func (e *EtcdBool) IsDelete() bool {
	return e == &deleteBoolOp
}
func (e *EtcdBytes) ToString() string {
	if e == nil {
		return ""
	}
	b := []byte(*e)
	return base64.RawURLEncoding.EncodeToString(b)
}
func (e *EtcdBytes) FromString(value string) error {
	b, err := base64.RawStdEncoding.DecodeString(value)
	if err != nil {
		return err
	}
	*e = EtcdBytes(b)
	return nil
}
func (e *EtcdBytes) IsGet() bool {
	return e == &getBytesOp
}
func (e *EtcdBytes) IsSet() bool {
	return e != &deleteBytesOp && e != nil
}
func (e *EtcdBytes) IsDelete() bool {
	return e == &deleteBytesOp
}

var getOp string = "get"
var deleteOp string = "delete"

const DateTimeFormat = "2005-01-01T13:03:03-0300"

func GetTime() *EtcdTime {
	return &getTimeOp
}
func DeleteTime() *EtcdTime {
	return &deleteTimeOp
}
func SetTime(t time.Time) *EtcdTime {
	e := EtcdTime(t)
	return &e
}

func GetUuid() *EtcdUuid {
	return &getUUIDOp
}
func DeleteUuid() *EtcdUuid {
	return &deleteUUIDOp
}
func SetUuid(u string) *EtcdUuid {
	s := EtcdUuid(u)
	return &s
}

func GetString() *EtcdString {
	return &getStringOp
}
func DeleteString() *EtcdString {
	return &deleteStringOp
}
func SetString(s string) *EtcdString {
	t := EtcdString(s)
	return &t
}

func GetInt() *EtcdInt {
	return &getIntOp
}
func DeleteInt() *EtcdInt {
	return &deleteIntOp
}
func SetInt(i int) *EtcdInt {
	j := EtcdInt(i)
	return &j
}

func GetUint() *EtcdUint {
	return &getUintOp
}
func DeleteUint() *EtcdUint {
	return &deleteUintOp
}
func SetUint(u uint) *EtcdUint {
	v := EtcdUint(u)
	return &v
}

func GetBool() *EtcdBool {
	return &getBoolOp
}
func DeleteBool() *EtcdBool {
	return &deleteBoolOp
}
func SetBool(b bool) *EtcdBool {
	c := EtcdBool(b)
	return &c
}

func GetBytes() *EtcdBytes {
	return &getBytesOp
}
func DeleteBytes() *EtcdBytes {
	return &deleteBytesOp
}
func SetBytes(b []byte) *EtcdBytes {
	c := EtcdBytes(b)
	return &c
}

// GenerateUniqueID will generate a uuid to be
// used as an ID in the data model
func GenerateUniqueID() string {
	return uuid.New().String()
}


package etcdclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testTime time.Time = time.Date(2018, 8, 30, 12, 0, 0, 0, time.UTC)
	uuid1    string    = GenerateUniqueID()
	uuid2    string    = GenerateUniqueID()
)

type TestModel2Child struct {
	BoolKey *EtcdBool `path:":@/bool_key"`
	IntKey  *EtcdInt  `path:":@/int_key"`
}

type TestModel2Parent struct {
	//CurrentTime *EtcdTime   `path:"/path/:var/to/key"`
	//SecondTime  *EtcdTime   `path:"/path/:var/to/second_key"`
	Name  *EtcdString `path:"/path/:var/to/name"`
	ID    *EtcdUuid   `path:"/path/:var/to/id"`
	Count *EtcdUint   `path:"/path/:var/to/count_key"`
	Child TestModel2Child  `path:"/path/:var/to/child"`
}

type TestModel3 struct {
	Name *EtcdString `path:"/path/test/:var/to/name"`
	IDs  []*EtcdUuid `path:"/path/test/:var/to/slice"`
}

type TestTimes struct {
	CurrentTime *EtcdTime `path:"/path/:var/to/time"`
}

func TestEtcdClient(t *testing.T) {
	/*
		NOTE: this test is not meant as a regression prevention test
		but rather an initial concept proving test. It requires running etcd
		locally (etcd& on a Debian based distro) and has a data race condition
		because the keys are not deleted from etcd after each test run.

		To validate manually the keys are set as expected in the local etcd instance
		you can run `export ETCDCTL_API=3 && etcdctl get --prefix /`

		This test covers the most basic functionality
		1.) pathvar substitution
		2.) Getting keys in a struct
		3.) Setting keys from a struct
		4.) Deleting keys
	*/
	cases := []struct {
		name         string
		pathvar      map[string]string
		dataToSet    TestModel2Parent
		dataToGet    TestModel2Parent
		expectedData TestModel2Parent
		expectedErr  error
	}{
		{
			name: "all_keys_set",
			dataToSet: TestModel2Parent{
				//CurrentTime: SetTime(testTime),
				//SecondTime:  SetTime(testTime),
				Name:  SetString("test"),
				ID:    SetUuid("uuid-test"),
				Count: SetUint(42),
				Child: TestModel2Child{
					BoolKey: SetBool(true),
					IntKey:  SetInt(42),
				},
			},
			dataToGet: TestModel2Parent{
				//CurrentTime: GetTime(),
				//SecondTime:  GetTime(),
				Name:  GetString(),
				ID:    GetUuid(),
				Count: GetUint(),
				Child: TestModel2Child{
					BoolKey: GetBool(),
					IntKey:  GetInt(),
				},
			},
			pathvar: map[string]string{
				"var": "sub",
			},
			expectedData: TestModel2Parent{
				//CurrentTime: SetTime(testTime),
				//SecondTime:  SetTime(testTime),
				Name:  SetString("test"),
				ID:    SetUuid("uuid-test"),
				Count: SetUint(42),
				Child: TestModel2Child{
					BoolKey: SetBool(true),
					IntKey:  SetInt(42),
				},
			},
		},
		{
			name: "partial_set",
			dataToSet: TestModel2Parent{
				//CurrentTime: SetTime(testTime),
				//SecondTime:  SetTime(testTime),
				Name:  SetString("test2"),
				ID:    SetUuid("uuid-test2"),
				Count: nil,
				Child: TestModel2Child{
					BoolKey: nil,
					IntKey:  SetInt(43),
				},
			},
			dataToGet: TestModel2Parent{
				//CurrentTime: GetTime(),
				//SecondTime:  GetTime(),
				Name:  GetString(),
				ID:    GetUuid(),
				Count: GetUint(),
				Child: TestModel2Child{
					BoolKey: GetBool(),
					IntKey:  GetInt(),
				},
			},
			pathvar: map[string]string{
				"var": "sub",
			},
			expectedData: TestModel2Parent{
				//CurrentTime: SetTime(testTime),
				//SecondTime:  SetTime(testTime),
				Name:  SetString("test2"),
				ID:    SetUuid("uuid-test2"),
				Count: SetUint(42),
				Child: TestModel2Child{
					BoolKey: SetBool(true),
					IntKey:  SetInt(43),
				},
			},
		},
		{
			name: "partial_delete",
			dataToSet: TestModel2Parent{
				//CurrentTime: SetTime(testTime),
				//SecondTime:  SetTime(testTime),
				Name:  DeleteString(),
				ID:    nil,
				Count: DeleteUint(),
				Child: TestModel2Child{
					BoolKey: nil,
					IntKey:  SetInt(44),
				},
			},
			dataToGet: TestModel2Parent{
				//CurrentTime: GetTime(),
				//SecondTime:  GetTime(),
				Name:  GetString(),
				ID:    GetUuid(),
				Count: GetUint(),
				Child: TestModel2Child{
					BoolKey: GetBool(),
					IntKey:  GetInt(),
				},
			},
			pathvar: map[string]string{
				"var": "sub",
			},
			expectedData: TestModel2Parent{
				//CurrentTime: SetTime(testTime),
				//SecondTime:  SetTime(testTime),
				ID: SetUuid("uuid-test2"),
				Child: TestModel2Child{
					BoolKey: SetBool(true),
					IntKey:  SetInt(44),
				},
			},
		},
		{
			name: "partial_get",
			dataToSet: TestModel2Parent{
				//CurrentTime: SetTime(testTime),
				//SecondTime:  SetTime(testTime),
				Name:  SetString("test"),
				ID:    SetUuid("uuid-test"),
				Count: SetUint(42),
				Child: TestModel2Child{
					BoolKey: SetBool(true),
					IntKey:  SetInt(42),
				},
			},
			dataToGet: TestModel2Parent{
				//CurrentTime: GetTime(),
				//SecondTime:  GetTime(),
				Name:  GetString(),
				Count: GetUint(),
				Child: TestModel2Child{
					IntKey: GetInt(),
				},
			},
			pathvar: map[string]string{
				"var": "sub",
			},
			expectedData: TestModel2Parent{
				//CurrentTime: SetTime(testTime),
				//SecondTime:  SetTime(testTime),
				Name:  SetString("test"),
				Count: SetUint(42),
				Child: TestModel2Child{
					IntKey: SetInt(42),
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := &config.GlobalConfig{
				Etcd: &config.EtcdConfig{
					Endpoints: []string{"http://localhost:2379"},
				},
			}

			store, err := NewEtcdStore(conf, config.GetLogger())
			require.NoError(t, err)
			defer store.Close()
			err = store.Set(&tc.dataToSet, tc.pathvar)
			if tc.expectedErr == nil {
				require.NoError(t, err)

				err = store.Get(&tc.dataToGet, tc.pathvar)
				require.NoError(t, err)
				assert.Equal(t, tc.dataToGet, tc.expectedData)
			} else {
				assert.Equal(t, tc.expectedErr, err)
			}
		})
	}
}

func TestEtcdClientTimeTypes(t *testing.T) {
	cases := []struct {
		name         string
		pathvar      map[string]string
		dataToSet    TestTimes
		dataToGet    TestTimes
		expectedData TestTimes
		expectedErr  error
	}{
		{
			name: "all_keys_set",
			dataToSet: TestTimes{
				CurrentTime: SetTime(testTime),
			},
			dataToGet: TestTimes{
				CurrentTime: GetTime(),
			},
			pathvar: map[string]string{
				"var": "sub",
			},
			expectedData: TestTimes{
				CurrentTime: SetTime(testTime),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := &config.GlobalConfig{
				Etcd: &config.EtcdConfig{
					Endpoints: []string{"http://localhost:2379"},
				},
			}

			store, err := NewEtcdStore(conf, config.GetLogger())
			require.NoError(t, err)
			defer store.Close()
			err = store.Set(&tc.dataToSet, tc.pathvar)
			if tc.expectedErr == nil {
				require.NoError(t, err)

				err = store.Get(&tc.dataToGet, tc.pathvar)
				require.NoError(t, err)
				assert.True(t, time.Time(*tc.dataToGet.CurrentTime).Equal(time.Time(*tc.expectedData.CurrentTime)))
			} else {
				assert.Equal(t, tc.expectedErr, err)
			}
		})
	}
}

func TestEtcdClientSlice(t *testing.T) {
	cases := []struct {
		name         string
		pathvar      map[string]string
		dataToSet    TestModel3
		dataToGet    TestModel3
		expectedData TestModel3
		expectedErr  error
	}{
		{
			name: "clear_slice",
			dataToSet: TestModel3{
				Name: SetString("testName"),
				IDs: []*EtcdUuid{
					DeleteUuid(),
				},
			},
			dataToGet: TestModel3{
				Name: GetString(),
				IDs: []*EtcdUuid{
					GetUuid(),
				},
			},
			pathvar: map[string]string{
				"var": "sub",
			},
			expectedData: TestModel3{
				Name: SetString("testName"),
				IDs:  []*EtcdUuid{},
			},
		},
		{
			name: "set_slice",
			dataToSet: TestModel3{
				Name: SetString("testName"),
				IDs: []*EtcdUuid{
					SetUuid(uuid1),
					SetUuid(uuid2),
				},
			},
			dataToGet: TestModel3{
				Name: GetString(),
				IDs: []*EtcdUuid{
					GetUuid(),
				},
			},
			pathvar: map[string]string{
				"var": "sub",
			},
			expectedData: TestModel3{
				Name: SetString("testName"),
				IDs: []*EtcdUuid{
					SetUuid(uuid1),
					SetUuid(uuid2),
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := &config.GlobalConfig{
				Etcd: &config.EtcdConfig{
					Endpoints: []string{"http://localhost:2379"},
				},
			}

			store, err := NewEtcdStore(conf, config.GetLogger())
			require.NoError(t, err)
			defer store.Close()
			err = store.Set(&tc.dataToSet, tc.pathvar)
			if tc.expectedErr == nil {
				require.NoError(t, err)

				err = store.Get(&tc.dataToGet, tc.pathvar)
				require.NoError(t, err)
				assert.Equal(t, tc.dataToGet, tc.expectedData)
			} else {
				assert.Equal(t, tc.expectedErr, err)
			}
		})
	}
}

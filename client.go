package etcdclient

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	tagKey = "path"
)

var (
	//emptyVal      *string
	etcdValueType reflect.Type = reflect.TypeOf((*EtcdValue)(nil)).Elem()
)

/*
TODO: improve functionality of client
1.) Array gets
2.) Encryption on sets
3.) Leases
4.) TLS for etcd client
*/

type store struct {
	logger *zap.Logger
	client *clientv3.Client
}

type Store interface {
	Get(g interface{}, pathvar map[string]string) error
	Set(s interface{}, pathvar map[string]string) error
	Close() error
}

// NewEtcdStore initializes a connection to etcd based on a passed in configuration
// Note: it is up to the initializer to close the store to not leak connections
func NewEtcdStore(conf *config.GlobalConfig, logger *zap.Logger) (Store, error) {
	if conf == nil || conf.Etcd == nil {
		return nil, fmt.Errorf("cannot pass in nil etcd configuration")
	}
	if logger == nil {
		logger = config.GetLogger()
	}

	c, err := clientv3.New(clientv3.Config{
		Endpoints: conf.Etcd.Endpoints,
	})
	if err != nil {
		logger.Error("error initializing etcd client", zap.Error(err))
		return nil, err
	}

	return &store{
		client: c,
		logger: logger,
	}, nil
}

func (c *store) Get(g interface{}, pathvar map[string]string) error {
	// Get the reflected value
	value := reflect.ValueOf(g)
	// Verify that the value is a pointer
	if value.Kind() != reflect.Ptr {
		err := fmt.Errorf("Provided interface is not a pointer")
		c.logger.Error("Error validating interface", zap.Error(err))
		return err
	}
	// Verify the pointer points to a struct
	value = value.Elem()
	if value.Kind() != reflect.Struct {
		err := fmt.Errorf("Provided interface is does not reference a struct")
		c.logger.Error("Error validating interface", zap.Error(err))
		return err
	}

	pathvar["@"] = ""

	etcdOps, callbacks, err := createStructGetOps(value, pathvar, false)
	if err != nil {
		c.logger.Error("Error generating ops", zap.Error(err))
		return err
	}

	txn := c.client.Txn(context.Background()).If().Then(etcdOps...)
	resp, err := txn.Commit()
	if err != nil {
		c.logger.Error("Error performing ops", zap.Error(err))
		return err
	}

	if len(resp.Responses) != len(callbacks) {
		err = fmt.Errorf("Unexpected number of responses")
		c.logger.Error("Invalid etcd response", zap.Error(err))
		return err
	}

	for i, r := range resp.Responses {
		if err = callbacks[i](r); err != nil {
			err = fmt.Errorf("Invalid data for field type")
			c.logger.Error("Error parsing etcd response", zap.Error(err))
			return err
		}
	}

	return nil
}

func createStructGetOps(value reflect.Value, pathvar map[string]string, inslice bool) ([]clientv3.Op, []func(*etcdserverpb.ResponseOp) error, error) {
	etcdOps := []clientv3.Op{}
	callbacks := []func(*etcdserverpb.ResponseOp) error{}

	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		// If the struct field has not path tag it will be ignored
		tag, ok := value.Type().Field(i).Tag.Lookup(tagKey)
		if !ok {
			continue
		}

		etcdKey, err := pathReplace(tag, pathvar, inslice)
		if err != nil {
			return nil, nil, err
		}

		if field.Kind() == reflect.Ptr && field.Type().Implements(etcdValueType) && field.Interface().(EtcdValue).IsGet() {
			etcdOps = append(etcdOps, clientv3.OpGet(etcdKey))
			callbacks = append(callbacks, func(resp *etcdserverpb.ResponseOp) error {
				val := reflect.New(field.Elem().Type())
				field.Set(val)
				iface, ok := val.Interface().(EtcdValue)
				if !ok {
					err = fmt.Errorf("Interface does not implement EtcdValue")
					return err
				}

				if len(resp.GetResponseRange().Kvs) <= 0 {
					field.Set(reflect.Zero(field.Type()))
					return nil
				}
				etcdVal := resp.GetResponseRange().Kvs[0].Value
				return iface.FromString(string(etcdVal))
			})
		} else if field.Kind() == reflect.Struct {
			pathvar["@"] = etcdKey
			newOps, newCallbacks, err := createStructGetOps(field, pathvar, inslice)
			if err != nil {
				return nil, nil, err
			}
			etcdOps = append(etcdOps, newOps...)
			callbacks = append(callbacks, newCallbacks...)
		} else if field.Kind() == reflect.Slice {
			pathvar["@"] = etcdKey
			newOps, newCallbacks, err := createSliceGetOps(field, etcdKey, true)
			if err != nil {
				return nil, nil, err
			}
			etcdOps = append(etcdOps, newOps...)
			callbacks = append(callbacks, newCallbacks...)
		}
	}

	return etcdOps, callbacks, nil
}

func createSliceGetOps(value reflect.Value, etcdKey string, inslice bool) ([]clientv3.Op, []func(*etcdserverpb.ResponseOp) error, error) {
	etcdOps := []clientv3.Op{}
	callbacks := []func(*etcdserverpb.ResponseOp) error{}

	if value.Len() < 1 {
		return etcdOps, callbacks, nil
	}
	field := value.Index(0)

	etcdKey = etcdKey + "/"

	if field.Kind() == reflect.Ptr && field.Type().Implements(etcdValueType) {
		if field.Interface().(EtcdValue).IsGet() {
			etcdOps = append(etcdOps, clientv3.OpGet(etcdKey, clientv3.WithPrefix()))
			callbacks = append(callbacks, func(resp *etcdserverpb.ResponseOp) error {
				// Clear the slice of the `Get` pointer
				value.Set(value.Slice(0, 0))
				for j := 0; j < len(resp.GetResponseRange().Kvs); j++ {
					// Create a new value to append into the slice
					etcdVal := string(resp.GetResponseRange().Kvs[j].Value)

					val := reflect.New(value.Type().Elem().Elem())
					iface, ok := val.Interface().(EtcdValue)
					if !ok {
						return fmt.Errorf("Interface does not implement EtcdValue")
					}

					err := iface.FromString(etcdVal)
					if err != nil {
						return err
					}

					value.Set(reflect.Append(value, val))
				}

				return nil
			})
		}
	} else {
		return nil, nil, fmt.Errorf("Slice must be of *EtcdValue type")
	}

	return etcdOps, callbacks, nil
}

func (c *store) Set(s interface{}, pathvar map[string]string) error {
	// Get the reflected value
	value := reflect.ValueOf(s)
	// Verify that the value is a pointer
	if value.Kind() != reflect.Ptr {
		err := fmt.Errorf("Provided interface is not a pointer")
		c.logger.Error("Error validating interface", zap.Error(err))
		return err
	}
	// Verify the pointer points to a struct
	value = value.Elem()
	if value.Kind() != reflect.Struct {
		err := fmt.Errorf("Provided interface is does not reference a struct")
		c.logger.Error("Error validating interface", zap.Error(err))
		return err
	}

	pathvar["@"] = ""

	etcdOps, err := createStructSetOps(value, pathvar, false)
	if err != nil {
		c.logger.Error("Error generating ops", zap.Error(err))
		return err
	}

	txn := c.client.Txn(context.Background()).If().Then(etcdOps...)
	resp, err := txn.Commit()
	if err != nil {
		c.logger.Error("Error performing ops", zap.Error(err))
		return err
	}

	if len(resp.Responses) != len(etcdOps) {
		err = fmt.Errorf("Unexpected number of responses")
		c.logger.Error("Invalid etcd response", zap.Error(err))
		return err
	}

	return nil
}

func createStructSetOps(value reflect.Value, pathvar map[string]string, inslice bool) ([]clientv3.Op, error) {
	etcdOps := []clientv3.Op{}

	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		// If the struct field has not path tag it will be ignored
		tag, ok := value.Type().Field(i).Tag.Lookup(tagKey)
		if !ok {
			continue
		}

		etcdKey, err := pathReplace(tag, pathvar, inslice)
		if err != nil {
			return nil, err
		}

		if field.Kind() == reflect.Ptr && field.Type().Implements(etcdValueType) {
			iface, ok := field.Interface().(EtcdValue)
			if !ok {
				err = fmt.Errorf("failed to cast interface")
				return nil, err
			}
			if iface.IsDelete() {
				etcdOps = append(etcdOps, clientv3.OpDelete(etcdKey))
			} else if iface.IsSet() {
				etcdOps = append(etcdOps, clientv3.OpPut(etcdKey, iface.ToString()))
			}
		} else if field.Kind() == reflect.Struct {
			pathvar["@"] = etcdKey
			newOps, err := createStructSetOps(field, pathvar, inslice)
			if err != nil {
				return nil, err
			}
			etcdOps = append(etcdOps, newOps...)
		} else if field.Kind() == reflect.Slice {
			newOps, err := createSliceSetOps(field, etcdKey, true)
			if err != nil {
				return nil, err
			}
			etcdOps = append(etcdOps, newOps...)
		}
	}

	return etcdOps, nil
}

func createSliceSetOps(value reflect.Value, etcdKey string, inslice bool) ([]clientv3.Op, error) {
	etcdOps := []clientv3.Op{}

	if value.Len() == 1 {
		field := value.Index(0)
		if field.Kind() == reflect.Ptr && field.Type().Implements(etcdValueType) && field.Interface().(EtcdValue).IsDelete() {
			// Delete all items in this slice
			etcdOps = append(etcdOps, clientv3.OpDelete(etcdKey+"/", clientv3.WithPrefix()))
			return etcdOps, nil
		}
	}

	for i := 0; i < value.Len(); i++ {
		field := value.Index(i)

		if field.Kind() == reflect.Ptr && field.Type().Implements(etcdValueType) && field.Interface().(EtcdValue).IsSet() {
			etcdKey = etcdKey + "/" + GenerateUniqueID()
			iface, ok := field.Interface().(EtcdValue)
			if !ok {
				err := fmt.Errorf("failed to cast interface")
				return nil, err
			}

			etcdOps = append(etcdOps, clientv3.OpPut(etcdKey, iface.ToString()))
		} else if field.Kind() == reflect.Struct {
			err := fmt.Errorf("Cannot set a slice of structs. Structs must be set individually")
			return nil, err
		}
	}

	return etcdOps, nil
}

func (c *store) Close() error {
	return c.client.Close()
}

// pathReplace replaces the stub values for the pathvars and returns
// the fully qualified path
func pathReplace(path string, pathvars map[string]string, inslice bool) (string, error) {
	pathArr := strings.Split(path, "/")
	for _, p := range pathArr {
		if strings.HasPrefix(p, ":") {
			variable := strings.TrimPrefix(p, ":")
			if variable == "@" && inslice {
				return "", fmt.Errorf("cannot use @ pathvar inside slice")
			}
			val, ok := pathvars[strings.TrimPrefix(p, ":")]
			if !ok {
				return "", fmt.Errorf("pathvar %s not populated", p)
			}
			path = strings.Replace(path, p, val, -1)
		}
	}

	return path, nil
}

// validateInterface validates the interface passed to the get or set command
// is a pointer to a struct
func validateInterface(v interface{}) error {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("cannot use not pointer structs")
	}
	return nil
}

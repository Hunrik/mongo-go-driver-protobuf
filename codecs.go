package codecs

import (
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson/bsontype"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/Hunrik/mongo-go-driver-protobuf/pmongo"
	"github.com/gogo/protobuf/types"
)

var (
	// Protobuf types types
	boolValueType   = reflect.TypeOf(types.BoolValue{})
	bytesValueType  = reflect.TypeOf(types.BytesValue{})
	doubleValueType = reflect.TypeOf(types.DoubleValue{})
	floatValueType  = reflect.TypeOf(types.FloatValue{})
	int32ValueType  = reflect.TypeOf(types.Int32Value{})
	int64ValueType  = reflect.TypeOf(types.Int64Value{})
	stringValueType = reflect.TypeOf(types.StringValue{})
	uint32ValueType = reflect.TypeOf(types.UInt32Value{})
	uint64ValueType = reflect.TypeOf(types.UInt64Value{})

	// Protobuf Timestamp type
	timestampType = reflect.TypeOf(timestamp.Timestamp{})

	// Time type
	timeType = reflect.TypeOf(time.Time{})

	// Struct type
	structType = reflect.TypeOf(types.Struct{})

	// ObjectId type
	objectIDType          = reflect.TypeOf(pmongo.ObjectId{})
	objectIDPrimitiveType = reflect.TypeOf(primitive.ObjectID{})

	// Codecs
	wrapperValueCodecRef = &wrapperValueCodec{}
	timestampCodecRef    = &timestampCodec{}
	objectIDCodecRef     = &objectIDCodec{}
	structCodecRef       = &structCodec{}
)

// structCodec is codec for Protobuf Struct
type structCodec struct {
}

// EncodeValue encodes Protobuf Struct wrapper value to BSON value
func (e *structCodec) EncodeValue(ectx bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	struc := val.Interface().(types.Struct)
	return writeStruct(vw, struc)
}

func writeStruct(vw bsonrw.ValueWriter, val types.Struct) error {
	docW, err := vw.WriteDocument()
	if err != nil {
		return err
	}
	defer docW.WriteDocumentEnd()

	if val.Fields == nil {
		return nil
	}

	for key, val := range val.Fields {
		valW, err := docW.WriteDocumentElement(key)
		if err != nil {
			return err
		}
		if err := writeField(valW, val); err != nil {
			return err
		}
	}
	return nil
}

func writeField(vw bsonrw.ValueWriter, val *types.Value) error {
	switch k := val.Kind.(type) {
	case *types.Value_NullValue:
		if err := vw.WriteNull(); err != nil {
			return err
		}
	case *types.Value_NumberValue:
		if err := vw.WriteDouble(k.NumberValue); err != nil {
			return err
		}
	case *types.Value_StringValue:
		if err := vw.WriteString(k.StringValue); err != nil {
			return err
		}
	case *types.Value_BoolValue:
		if err := vw.WriteBoolean(k.BoolValue); err != nil {
			return err
		}
	case *types.Value_StructValue:
		return writeStruct(vw, *k.StructValue)
	case *types.Value_ListValue:
		arrW, err := vw.WriteArray()
		if err != nil {
			return err
		}
		vw, err := arrW.WriteArrayElement()
		if err != nil {
			return err
		}
		s := make([]interface{}, len(k.ListValue.Values))
		for i, e := range k.ListValue.Values {
			s[i] = writeField(vw, e)
		}
		if err := arrW.WriteArrayEnd(); err != nil {
			return err
		}
	default:
		panic("unknown kind")
	}
	return nil
}

// DecodeValue decodes BSON value to Protobuf Struct type value
func (e *structCodec) DecodeValue(ectx bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	docread, err := vr.ReadDocument()
	if err != nil {
		return err
	}
	res, err := readStruct(docread)
	if err != nil {
		return err
	}
	val.Set(reflect.ValueOf(*ToStruct(res)))
	return nil
}

func readStruct(docreader bsonrw.DocumentReader) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	for {
		key, valueReader, err := docreader.ReadElement()
		if err != nil {
			break
		}
		switch valueReader.Type() {
		case bsontype.String:
			str, err := valueReader.ReadString()
			if err != nil {
				return nil, err
			}
			res[key] = str
		case bsontype.Double:
			val, err := valueReader.ReadDouble()
			if err != nil {
				return nil, err
			}
			res[key] = val
		case bsontype.Int32:
			val, err := valueReader.ReadInt32()
			if err != nil {
				return nil, err
			}
			res[key] = val
		case bsontype.Int64:
			val, err := valueReader.ReadInt64()
			if err != nil {
				return nil, err
			}
			res[key] = val
		case bsontype.Boolean:
			val, err := valueReader.ReadBoolean()
			if err != nil {
				return nil, err
			}
			res[key] = val
		case bsontype.Null:
			if err := valueReader.ReadNull(); err != nil {
				return nil, err
			}
			res[key] = nil
		case bsontype.EmbeddedDocument:
			doc, err := valueReader.ReadDocument()
			if err != nil {
				return nil, err
			}
			res[key], err = readStruct(doc)
			if err != nil {
				return nil, err
			}
		default:
			panic("Unknown type")
		}
	}
	return res, nil
}

// wrapperValueCodec is codec for Protobuf type types
type wrapperValueCodec struct {
}

// EncodeValue encodes Protobuf type wrapper value to BSON value
func (e *wrapperValueCodec) EncodeValue(ectx bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	val = val.FieldByName("Value")
	enc, err := ectx.LookupEncoder(val.Type())
	if err != nil {
		return err
	}
	return enc.EncodeValue(ectx, vw, val)
}

// DecodeValue decodes BSON value to Protobuf type wrapper value
func (e *wrapperValueCodec) DecodeValue(ectx bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	val = val.FieldByName("Value")
	enc, err := ectx.LookupDecoder(val.Type())
	if err != nil {
		return err
	}
	return enc.DecodeValue(ectx, vr, val)
}

// timestampCodec is codec for Protobuf Timestamp
type timestampCodec struct {
}

// EncodeValue encodes Protobuf Timestamp value to BSON value
func (e *timestampCodec) EncodeValue(ectx bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	v := val.Interface().(timestamp.Timestamp)
	t, err := ptypes.Timestamp(&v)
	if err != nil {
		return err
	}
	enc, err := ectx.LookupEncoder(timeType)
	if err != nil {
		return err
	}
	return enc.EncodeValue(ectx, vw, reflect.ValueOf(t.In(time.UTC)))
}

// DecodeValue decodes BSON value to Timestamp value
func (e *timestampCodec) DecodeValue(ectx bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	enc, err := ectx.LookupDecoder(timeType)
	if err != nil {
		return err
	}
	var t time.Time
	if err = enc.DecodeValue(ectx, vr, reflect.ValueOf(&t).Elem()); err != nil {
		return err
	}
	ts, err := ptypes.TimestampProto(t.In(time.UTC))
	if err != nil {
		return err
	}
	val.Set(reflect.ValueOf(*ts))
	return nil
}

// objectIDCodec is codec for Protobuf ObjectId
type objectIDCodec struct {
}

// EncodeValue encodes Protobuf ObjectId value to BSON value
func (e *objectIDCodec) EncodeValue(ectx bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	v := val.Interface().(pmongo.ObjectId)
	// Create primitive.ObjectId from string
	id, err := primitive.ObjectIDFromHex(v.Value)
	if err != nil {
		return err
	}
	enc, err := ectx.LookupEncoder(objectIDPrimitiveType)
	if err != nil {
		return err
	}
	return enc.EncodeValue(ectx, vw, reflect.ValueOf(id))
}

// DecodeValue decodes BSON value to ObjectId value
func (e *objectIDCodec) DecodeValue(ectx bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	enc, err := ectx.LookupDecoder(objectIDPrimitiveType)
	if err != nil {
		return err
	}
	var id primitive.ObjectID
	if err = enc.DecodeValue(ectx, vr, reflect.ValueOf(&id).Elem()); err != nil {
		return err
	}
	oid := *pmongo.NewObjectId(id)
	if err != nil {
		return err
	}
	val.Set(reflect.ValueOf(oid))
	return nil
}

// Register registers Google protocol buffers types codecs
func Register(rb *bsoncodec.RegistryBuilder) *bsoncodec.RegistryBuilder {
	return rb.RegisterCodec(boolValueType, wrapperValueCodecRef).
		RegisterCodec(bytesValueType, wrapperValueCodecRef).
		RegisterCodec(doubleValueType, wrapperValueCodecRef).
		RegisterCodec(floatValueType, wrapperValueCodecRef).
		RegisterCodec(int32ValueType, wrapperValueCodecRef).
		RegisterCodec(int64ValueType, wrapperValueCodecRef).
		RegisterCodec(stringValueType, wrapperValueCodecRef).
		RegisterCodec(uint32ValueType, wrapperValueCodecRef).
		RegisterCodec(uint64ValueType, wrapperValueCodecRef).
		RegisterCodec(timestampType, timestampCodecRef).
		RegisterCodec(structType, structCodecRef).
		RegisterCodec(objectIDType, objectIDCodecRef)
}

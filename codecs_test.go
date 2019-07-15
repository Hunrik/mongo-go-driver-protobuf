package codecs

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	assert "github.com/stretchr/testify/require"

	"github.com/gogo/protobuf/jsonpb"

	"github.com/gogo/protobuf/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/Hunrik/mongo-go-driver-protobuf/pmongo"
	"github.com/Hunrik/mongo-go-driver-protobuf/test"
)

func TestCodecs(t *testing.T) {
	rb := bson.NewRegistryBuilder()
	r := Register(rb).Build()

	tm := time.Now()
	// BSON accuracy is in milliseconds
	tm = time.Date(tm.Year(), tm.Month(), tm.Day(), tm.Hour(), tm.Minute(), tm.Second(),
		(tm.Nanosecond()/1000000)*1000000, tm.Location())

	ts, err := types.TimestampProto(tm)
	assert.NoError(t, err)

	objectID := primitive.NewObjectID()
	id := pmongo.NewObjectId(objectID)

	t.Run("primitive object id", func(t *testing.T) {
		resultID, err := id.GetObjectID()
		if err != nil {
			t.Errorf("mongodb.ObjectId.GetPrimitiveObjectID() error = %v", err)
			return
		}

		if !reflect.DeepEqual(objectID, resultID) {
			t.Errorf("failed: primitive object ID=%#v, ID=%#v", objectID, id)
			return
		}
	})

	in := test.Data{
		BoolValue:   &types.BoolValue{Value: true},
		BytesValue:  &types.BytesValue{Value: make([]byte, 5)},
		DoubleValue: &types.DoubleValue{Value: 1.2},
		FloatValue:  &types.FloatValue{Value: 1.3},
		Int32Value:  &types.Int32Value{Value: -12345},
		Int64Value:  &types.Int64Value{Value: -123456789},
		StringValue: &types.StringValue{Value: "qwerty"},
		Uint32Value: &types.UInt32Value{Value: 12345},
		Uint64Value: &types.UInt64Value{Value: 123456789},
		StructValue: &types.Struct{
			Fields: map[string]*types.Value{
				"string": &types.Value{
					Kind: &types.Value_StringValue{
						StringValue: "foo",
					},
				},
				"int": &types.Value{
					Kind: &types.Value_NumberValue{
						NumberValue: 64,
					},
				},
				"bool": &types.Value{
					Kind: &types.Value_BoolValue{
						BoolValue: true,
					},
				},
				"substruct": &types.Value{
					Kind: &types.Value_StructValue{
						StructValue: &types.Struct{
							Fields: map[string]*types.Value{
								"subfoo": &types.Value{
									Kind: &types.Value_StringValue{
										StringValue: "bar",
									},
								},
								"subnum": &types.Value{
									Kind: &types.Value_NumberValue{
										NumberValue: 42.42,
									},
								},
							},
						},
					},
				},
			},
		},
		EmptyStructValue: &types.Struct{
			Fields: map[string]*types.Value{},
		},
		Timestamp: ts,
	}

	t.Run("marshal/unmarshal", func(t *testing.T) {
		b, err := bson.MarshalWithRegistry(r, &in)
		if err != nil {
			t.Errorf("bson.MarshalWithRegistry error = %v", err)
			return
		}

		var out test.Data

		if err = bson.UnmarshalWithRegistry(r, b, &out); err != nil {
			t.Errorf("bson.UnmarshalWithRegistry error = %v", err)
			return
		}

		assert.Equal(t, in, out)
	})

	t.Run("marshal-jsonpb/unmarshal-jsonpb", func(t *testing.T) {
		var b bytes.Buffer

		m := &jsonpb.Marshaler{}

		err := m.Marshal(&b, &in)
		assert.NoError(t, err)

		var out test.Data
		err = jsonpb.Unmarshal(&b, &out)
		assert.NoError(t, err)

		assert.Equal(t, in, out)
	})
}

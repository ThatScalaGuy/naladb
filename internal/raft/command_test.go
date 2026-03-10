package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

func TestCommandType_String(t *testing.T) {
	tests := []struct {
		ct   CommandType
		want string
	}{
		{CmdSet, "CmdSet"},
		{CmdDelete, "CmdDelete"},
		{CmdCreateNode, "CmdCreateNode"},
		{CmdCreateEdge, "CmdCreateEdge"},
		{CmdBatchSet, "CmdBatchSet"},
		{CommandType(99), "CommandType(99)"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, tt.ct.String())
	}
}

func TestMarshalUnmarshalCommand(t *testing.T) {
	ts := hlc.NewHLC(1000, 1, 0)

	cmd := RaftCommand{
		Type: CmdSet,
		Ops: []KVOp{
			{Key: "mykey", Value: []byte("myvalue"), HLC: ts},
		},
	}

	data, err := MarshalCommand(cmd)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	got, err := UnmarshalCommand(data)
	require.NoError(t, err)
	assert.Equal(t, cmd.Type, got.Type)
	require.Len(t, got.Ops, 1)
	assert.Equal(t, "mykey", got.Ops[0].Key)
	assert.Equal(t, []byte("myvalue"), got.Ops[0].Value)
	assert.Equal(t, ts, got.Ops[0].HLC)
}

func TestMarshalUnmarshalCommand_BatchSet(t *testing.T) {
	cmd := RaftCommand{
		Type: CmdBatchSet,
		Ops: []KVOp{
			{Key: "k1", Value: []byte("v1"), HLC: hlc.NewHLC(100, 0, 0)},
			{Key: "k2", Value: []byte("v2"), HLC: hlc.NewHLC(200, 0, 0)},
			{Key: "k3", Value: []byte("v3"), HLC: hlc.NewHLC(300, 0, 0)},
		},
	}

	data, err := MarshalCommand(cmd)
	require.NoError(t, err)

	got, err := UnmarshalCommand(data)
	require.NoError(t, err)
	assert.Equal(t, CmdBatchSet, got.Type)
	assert.Len(t, got.Ops, 3)
}

func TestMarshalUnmarshalCommand_Delete(t *testing.T) {
	cmd := RaftCommand{
		Type: CmdDelete,
		Ops: []KVOp{
			{Key: "mykey", HLC: hlc.NewHLC(500, 0, 0), Tombstone: true},
		},
	}

	data, err := MarshalCommand(cmd)
	require.NoError(t, err)

	got, err := UnmarshalCommand(data)
	require.NoError(t, err)
	assert.True(t, got.Ops[0].Tombstone)
}

func TestUnmarshalCommand_Invalid(t *testing.T) {
	_, err := UnmarshalCommand([]byte("not json"))
	assert.Error(t, err)
}

func TestMarshalUnmarshalCommand_BlobRef(t *testing.T) {
	cmd := RaftCommand{
		Type: CmdSet,
		Ops: []KVOp{
			{Key: "blobkey", Value: []byte("sha256ref"), HLC: hlc.NewHLC(100, 0, 0), BlobRef: true},
		},
	}

	data, err := MarshalCommand(cmd)
	require.NoError(t, err)

	got, err := UnmarshalCommand(data)
	require.NoError(t, err)
	assert.True(t, got.Ops[0].BlobRef)
}

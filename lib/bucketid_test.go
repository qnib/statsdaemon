package statsdaemon

import (
	"github.com/qnib/qframe-types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBucketID_GenerateID(t *testing.T) {
	bid := BucketID{
		ID:         "",
		BucketName: "bucketName",
		Dimensions: qtypes.NewDimensions(),
	}
	bid.GenerateID()
	assert.Equal(t, "6ca9aa6888d75921ef87627bbd9719772aaff56f", bid.ID)
	assert.Panics(t, bid.GenerateID, "Should panic")
}

func TestBucketID_GenerateID_WithDims(t *testing.T) {
	dims := qtypes.NewDimensions()
	dims.Add("key1", "val1")
	bid := BucketID{
		ID:         "",
		BucketName: "bucketName",
		Dimensions: dims,
	}
	bid.GenerateID()
	assert.Equal(t, "2af96db5523ec73ecccef75192990635df2067b5", bid.ID)
	dims.Add("key2", "val2")
	bid.ID = ""
	bid.Dimensions = dims
	bid.GenerateID()
	assert.Equal(t, "23fe036dd9a06100c8056bc26a27f07ce9b601d4", bid.ID)

}

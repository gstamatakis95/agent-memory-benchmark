package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadCustomDataset(t *testing.T) {
	convs, err := loadDataset("../../testdata/custom-example.json")
	require.NoError(t, err)
	require.Len(t, convs, 2)
	require.Equal(t, "team-standup", convs[0].Name)
	require.Len(t, convs[0].Items, 4)
	require.Len(t, convs[0].Questions, 2)
	require.Equal(t, "custom-example", convs[0].Questions[0].Group)
	// Ids are salted by the dataset's base name, so host path and container
	// path agree, and the two conversations never collide.
	require.Equal(t, surrogateConvID("custom-example", "team-standup"), convs[0].Num)
	require.NotEqual(t, convs[0].Num, convs[1].Num)
	// A relocated copy of the same file yields the same ids.
	require.Equal(t, "custom-example", datasetName("/app/datasets/custom-example.json"))

	// The single-conversation fixtures shape loads through the same path.
	single, err := loadDataset("../../testdata/fixtures.json")
	require.NoError(t, err)
	require.Len(t, single, 1)
	require.Equal(t, "fixtures", single[0].Name)
	require.NotEmpty(t, single[0].Questions)

	_, err = loadDataset("../../testdata/does-not-exist.json")
	require.Error(t, err)
	require.False(t, isCustomDataset("locomo"))
	require.True(t, isCustomDataset("datasets/mine.json"))
}

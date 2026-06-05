package storage

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
	"sync"
)

var assetWordList = []string{
	"amber", "anchor", "apricot", "aster", "autumn", "badger", "bamboo", "banner", "barley", "basil",
	"bayou", "beacon", "birch", "blossom", "bramble", "breeze", "brook", "canyon", "cedar", "cinder",
	"clover", "comet", "coral", "cove", "cricket", "dahlia", "dawn", "delta", "drift", "dune",
	"ember", "falcon", "fern", "field", "finch", "fjord", "flint", "forest", "galaxy", "garden",
	"glade", "granite", "harbor", "hazel", "heather", "heron", "hollow", "iris", "ivy", "juniper",
	"lagoon", "laurel", "linen", "lumen", "maple", "marble", "meadow", "merlin", "meteor", "mist",
	"monarch", "moss", "nectar", "nova", "oasis", "olive", "onyx", "orchid", "otter", "petal",
	"pine", "prairie", "quartz", "raven", "reef", "river", "robin", "saffron", "sage", "sierra",
	"solstice", "sparrow", "spruce", "stone", "summit", "sunrise", "thicket", "timber", "topaz", "vale",
	"velvet", "violet", "willow", "wind", "wren", "yarrow", "zephyr",
}

var assetExtensionList = []string{
	"amber", "arc", "bloom", "cairn", "cinder", "clay", "cove", "dawn", "dune", "fern",
	"field", "fjord", "glow", "grove", "harbor", "haze", "iris", "jade", "lark", "linen",
	"meadow", "mist", "moss", "nova", "ocean", "opal", "quill", "reef", "sage", "shore",
	"sky", "spruce", "stone", "vale", "wave", "willow", "wisp",
}

var assetSeparators = []string{"-", "_", ""}

type assetNamer struct {
	mu   sync.Mutex
	used map[string]struct{}
}

func newAssetNamer() *assetNamer {
	return &assetNamer{used: make(map[string]struct{})}
}

func (n *assetNamer) Next() (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for attempts := 0; attempts < 32; attempts++ {
		name, err := randomAssetName()
		if err != nil {
			return "", err
		}
		if _, exists := n.used[name]; exists {
			continue
		}
		n.used[name] = struct{}{}
		return name, nil
	}
	return "", fmt.Errorf("generate unique asset name")
}

func randomAssetName() (string, error) {
	wordCount, err := randomInt(3)
	if err != nil {
		return "", err
	}
	wordCount += 2
	separator, err := randomChoice(assetSeparators)
	if err != nil {
		return "", err
	}
	parts := make([]string, wordCount)
	for i := range parts {
		parts[i], err = randomChoice(assetWordList)
		if err != nil {
			return "", err
		}
	}
	ext, err := randomChoice(assetExtensionList)
	if err != nil {
		return "", err
	}
	return strings.Join(parts, separator) + "." + ext, nil
}

func randomChoice(values []string) (string, error) {
	idx, err := randomInt(len(values))
	if err != nil {
		return "", err
	}
	return values[idx], nil
}

func randomInt(max int) (int, error) {
	if max <= 0 {
		return 0, fmt.Errorf("invalid random bound %d", max)
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0, fmt.Errorf("generate random int: %w", err)
	}
	return int(n.Int64()), nil
}

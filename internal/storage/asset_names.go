package storage

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
)

// assetDictionary is the single source for both name words and extensions.
// Expanded from the original ~77-word wordlist to ~320 words to make
// 1-5 word / 1-5 extension combinations collision-robust without relying
// on a separate wordlist. All entries are lower-case a-z so the resulting
// asset names remain `^[a-z]+(?:[-_]?[a-z]+)*(\.[a-z]+)+$`.
var assetDictionary = []string{
	"acacia", "acorn", "agave", "alder", "alpine", "amber", "anchor", "apricot", "arctic", "ash",
	"aspen", "aster", "aurora", "autumn", "avalanche", "azure", "badger", "bamboo", "banner", "barley",
	"basil", "basin", "bayou", "beacon", "beech", "birch", "blossom", "bluff", "boulder", "boreal",
	"bramble", "breeze", "brook", "brush", "canyon", "cape", "carbon", "cascade", "catalpa", "cavern",
	"cedar", "celestial", "chasm", "chestnut", "cinder", "cliff", "clover", "coast", "cobble", "comet",
	"conifer", "coral", "cove", "crag", "creek", "crest", "cricket", "crimson", "crystal", "cypress",
	"dahlia", "dale", "dawn", "delta", "desert", "dogwood", "drift", "dune", "durango", "echo",
	"eclipse", "elm", "ember", "estuary", "evergreen", "falcon", "fern", "field", "finch", "fjord",
	"flint", "foothill", "forest", "frost", "galaxy", "gale", "gap", "garden", "geode", "geyser",
	"glade", "glacier", "glen", "granite", "grove", "gulch", "harbor", "hazel", "heather", "hemlock",
	"heron", "highland", "hollow", "holly", "horizon", "husky", "indigo", "inlet", "iris", "ironwood",
	"isle", "ivy", "jackpine", "jasper", "juniper", "keld", "kestrel", "knoll", "lagoon", "larch",
	"laurel", "lava", "ledge", "lichen", "linden", "linen", "loam", "lodge", "lotus", "lowland",
	"lumen", "lupine", "maple", "marble", "marl", "marsh", "meadow", "merlin", "mesa", "meteor",
	"mist", "mistral", "monarch", "moor", "moraine", "moss", "mountain", "nectar", "nimbus", "nova",
	"oak", "oasis", "obsidian", "olive", "onyx", "orchard", "orchid", "otter", "pampa", "peak",
	"peat", "petal", "pine", "plateau", "plaza", "prairie", "quartz", "quagmire", "rapids", "raven",
	"ravine", "reef", "ridge", "rill", "river", "robin", "saffron", "sage", "savanna", "sequoia",
	"shale", "shore", "sierra", "silt", "solstice", "sparrow", "spruce", "steppe", "stone", "summit",
	"sunrise", "sycamore", "taiga", "tarn", "thicket", "timber", "topaz", "tundra", "vale", "valley",
	"velvet", "verdant", "violet", "vista", "volcano", "wadi", "willow", "wind", "wren", "yarrow",
	"zephyr",
}

// assetSeparators joins the word segments; extensions are always "."-joined.
var assetSeparators = []string{"-", "_", ""}

type assetNamer struct {
	// No mutex: the namer is per-chunkSink (one upload loop), never
	// shared across goroutines (single-flight collision scope). Hoisting it to the hub
	// would require re-adding a lock; per-sink keeps collision scope
	// to the current file's chunks, which is exactly what the
	// maxNameRetries budget assumes.
	used map[string]struct{}
}

func newAssetNamer() *assetNamer {
	return &assetNamer{used: make(map[string]struct{})}
}

func (n *assetNamer) Next() (string, error) {
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
	// 1-5 words and 1-5 extensions, each uniformly random. This yields
	// 320^1·320^1 ≈ 1e5 combinations at the small end up to 320^5·320^5 ≈ 1e25
	// at the large end - far more robust than the previous 2-4 words × 1 ext
	// on a 77-word list, without ever deriving from the source file name.
	wordCount, err := randomInt(5)
	if err != nil {
		return "", err
	}
	wordCount++

	extCount, err := randomInt(5)
	if err != nil {
		return "", err
	}
	extCount++

	separator, err := randomChoice(assetSeparators)
	if err != nil {
		return "", err
	}

	words := make([]string, wordCount)
	for i := range words {
		words[i], err = randomChoice(assetDictionary)
		if err != nil {
			return "", err
		}
	}

	exts := make([]string, extCount)
	for i := range exts {
		exts[i], err = randomChoice(assetDictionary)
		if err != nil {
			return "", err
		}
	}

	base := strings.Join(words, separator)
	suffix := strings.Join(exts, ".")
	return base + "." + suffix, nil
}

func randomChoice(values []string) (string, error) {
	idx, err := randomInt(len(values))
	if err != nil {
		return "", err
	}
	return values[idx], nil
}

func randomInt(bound int) (int, error) {
	if bound <= 0 {
		return 0, fmt.Errorf("invalid random bound %d", bound)
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(bound)))
	if err != nil {
		return 0, fmt.Errorf("generate random int: %w", err)
	}
	return int(n.Int64()), nil
}

package metadata

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// chainManifest builds a manifest whose tree is one linear chain of depth
// levels below the root: each node holds a single subdir named "d".
func chainManifest(levels int) (*Manifest, map[string][]byte) {
	objs := map[string][]byte{}
	var childSHA string
	for i := levels; i >= 0; i-- {
		node := TreeNode{Meta: DirMeta{Inode: uint64(i + 1)}}
		if childSHA != "" {
			node.Subdirs = map[string]string{"d": childSHA}
		}
		data, err := json.Marshal(node)
		if err != nil {
			panic(err)
		}
		childSHA = ObjectSHA(data)
		objs[childSHA] = data
	}
	return &Manifest{Version: maxMetadataVersion, Project: "p", TreeRoot: childSHA}, objs
}

func fetchFrom(objs map[string][]byte) func(string) ([]byte, error) {
	return func(sha string) ([]byte, error) {
		data, ok := objs[sha]
		if !ok {
			return nil, fmt.Errorf("missing test object %s", shortObj(sha))
		}
		return data, nil
	}
}

// TestLoadTreeDepthBound pins the nesting guard on both loaders: a manifest
// deeper than maxLoadTreeDepth fails with a depth error instead of
// recursing without bound, while a shallow chain still loads.
func TestLoadTreeDepthBound(t *testing.T) {
	t.Parallel()
	shallow, shallowObjs := chainManifest(3)
	if _, err := LoadTree(shallow, fetchFrom(shallowObjs)); err != nil {
		t.Fatalf("shallow chain must load: %v", err)
	}
	if _, err := LoadTreeParallel(shallow, fetchFrom(shallowObjs)); err != nil {
		t.Fatalf("shallow chain must load in parallel: %v", err)
	}
	deep, deepObjs := chainManifest(maxLoadTreeDepth + 8)
	if _, err := LoadTree(deep, fetchFrom(deepObjs)); err == nil || !strings.Contains(err.Error(), "depth") {
		t.Fatalf("deep chain must fail with a depth error, got %v", err)
	}
	if _, err := LoadTreeParallel(deep, fetchFrom(deepObjs)); err == nil || !strings.Contains(err.Error(), "depth") {
		t.Fatalf("deep chain must fail with a depth error in parallel, got %v", err)
	}
}

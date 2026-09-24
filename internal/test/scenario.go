package test

import "slices"

// Surface selector bitmask for Scenario.Surfaces.
const (
	// SurfaceFUSE selects the FUSE mount surface.
	SurfaceFUSE uint32 = 1 << iota
	// SurfaceREST selects the REST API surface.
	SurfaceREST
	// SurfaceCLI selects the CLI surface.
	SurfaceCLI
)

// SurfaceAll selects every known surface.
const SurfaceAll = SurfaceFUSE | SurfaceREST | SurfaceCLI

// Scenario is one deterministic conformance check against a Surface.
// Each Run must use its own unique paths and avoid sleeps and wall-clock reads.
type Scenario struct {
	Name     string
	Surfaces uint32 // bitmask: SurfaceFUSE, SurfaceREST, SurfaceCLI, SurfaceAll
	Run      func(Surface) error
}

// Table notes (contract divergences documented, not faked).
//
//   - Open carries creation intent in disp, like O_CREAT on open(2):
//     portable scenarios create with CreateFile first, then open with
//     CreateNever, and a write-mode open of a missing path with
//     CreateNever expects ErrNotFound (POSIX ENOENT without O_CREAT),
//     which the MemSurface oracle enforces. The FUSE, REST and CLI
//     adapters honor disp through their write-mode create paths (a
//     missing file under CreateIfMissing is created with mode 0o644
//     before opening; OpenReadOnly and OpenPath never create), so
//     creation is one spelled contract, not a divergence.
//   - Chown pins the non-privileged rule only: any successful chown in
//     the table must clear setuid/setgid (cross-owner chown is EPERM on
//     enforcing surfaces and cannot be portable, so scenarios reassert
//     the current owner). The admin-keeps-bits exemption lives outside
//     the interface (MemSurface.ChownAdmin) with its real proof in the
//     storage-layer unit tests, which own caller identity.
//   - umask is expressed through the optional UmaskSurface capability:
//     SetUmask configures the creation mask that CreateFile (and files
//     created by Open under CreateIfMissing) must observe, pinned by
//     the umask-masks-create-mode scenario. The fixed 022 default plus
//     the --umask mount/serve override stays pinned at the product
//     layer by TestMountUmaskFlagMasksCreatedModes
//     (internal/cli/app_test.go) and TestCallerContextCarriesDefaultUmask
//     (internal/fusefs/fuse_dac_test.go): the conformance mask starts
//     at zero and each scenario restores it, so product defaults never
//     leak into the table.
//   - Scratch multi-name rows (link-two-names-both-publish and
//     siblings) run on the oracle and on the REST and CLI adapters
//     (Surfaces SurfaceREST | SurfaceCLI; the oracle runs the full
//     table unfiltered, and both fakes wire test.PendingNames into
//     their session tables).
//
// Table is the full conformance suite.
var Table = slices.Concat(
	scenarioBasic,
	scenarioMetadata,
	scenarioLifecycle,
	scenarioScratch,
	scenarioNamespace,
	scenarioIO,
	scenarioDirOpen,
	scenarioSeek,
	scenarioScratchMulti,
)

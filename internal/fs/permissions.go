package fs

import (
	"context"
	"os"
	"slices"
	"syscall"
	"time"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

type Identity struct {
	UID    uint32
	GID    uint32
	PID    uint32
	Admin  bool
	Umask  uint32
	Groups []uint32
}

type contextKey string

const (
	identityContextKey   contextKey = "storhub.identity"
	createModeContextKey contextKey = "storhub.create_mode"
	accessRead                      = 0o4
	accessWrite                     = 0o2
	accessExec                      = 0o1
)

const (
	AccessRead  = accessRead
	AccessWrite = accessWrite
	AccessExec  = accessExec
)

type nodeAttrs struct {
	Path  string
	Mode  uint32
	UID   uint32
	GID   uint32
	IsDir bool
	Kind  meta.NodeKind
}

type createMode struct {
	mode uint32
	set  bool
}

func WithIdentity(ctx context.Context, id Identity) context.Context {
	id.Groups = uniqueGIDs(id.Groups)
	if id.GID != 0 {
		id.Groups = uniqueGIDs(append(id.Groups, id.GID))
	}
	// Admin is an explicit assertion only: a UID-0 identity without
	// Admin:true stays unprivileged (DAC still keys off the UID, so
	// root-owned files keep their owner semantics). The earlier silent
	// promotion made the Admin flag on uid-0 accounts meaningless, so a
	// configured admin:false still yielded full admin. Callers that mean
	// privileged (FUSE uid-0 mounts, REST admin records) set Admin:true
	// at the assertion point; the process-user fallback in
	// IdentityFromContext never mints it.
	return context.WithValue(ctx, identityContextKey, id)
}

func IdentityFromContext(ctx context.Context) Identity {
	if ctx != nil {
		if id, ok := ctx.Value(identityContextKey).(Identity); ok {
			return normalizeIdentity(id)
		}
	}
	// Fail closed: an absent identity means the local process operating its
	// own repository - never an anonymous superuser. Multi-user surfaces
	// (FUSE, REST) must attach the real caller via WithIdentity; the
	// fallback deliberately carries no Admin even when the daemon runs as
	// root, because the *process* uid is not a caller assertion.
	return Identity{UID: uint32(os.Getuid()), GID: uint32(os.Getgid())}
}

func IdentityPresent(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	_, ok := ctx.Value(identityContextKey).(Identity)
	return ok
}

func OwnerIDsForCreate(ctx context.Context, fallbackUID, fallbackGID uint32) (uint32, uint32) {
	if !IdentityPresent(ctx) {
		return fallbackUID, fallbackGID
	}
	id := IdentityFromContext(ctx)
	return id.UID, id.GID
}

func WithCreateMode(ctx context.Context, mode uint32) context.Context {
	return context.WithValue(ctx, createModeContextKey, createMode{mode: mode & 0o7777, set: true})
}

func CreateModeFromContext(ctx context.Context) (uint32, bool) {
	if ctx == nil {
		return 0, false
	}
	mode, ok := ctx.Value(createModeContextKey).(createMode)
	if !ok || !mode.set {
		return 0, false
	}
	return mode.mode, true
}

func ApplyCreateMode(ctx context.Context, fallback uint32) uint32 {
	id := IdentityFromContext(ctx)
	mode := fallback & 0o7777
	if requested, ok := CreateModeFromContext(ctx); ok {
		mode = requested & 0o7777
	}
	return mode &^ (id.Umask & 0o7777)
}

func CheckReadAccess(ctx context.Context, repo *meta.RepoMetadata, filePath string) error {
	return checkPathAccess(ctx, repo, filePath, accessRead)
}

func CheckWriteAccess(ctx context.Context, repo *meta.RepoMetadata, filePath string) error {
	return checkPathAccess(ctx, repo, filePath, accessWrite)
}

func CheckListDirAccess(ctx context.Context, repo *meta.RepoMetadata, dirPath string) error {
	if err := CheckWalk(ctx, repo, dirPath); err != nil {
		return err
	}
	attrs, err := lookupNode(repo, dirPath)
	if err != nil {
		return err
	}
	if !attrs.IsDir {
		return syscall.ENOTDIR
	}
	return checkAccess(IdentityFromContext(ctx), attrs, accessRead|accessExec)
}

// CheckReadAccessResolved is CheckReadAccess for a path that
// ResolveAccessPath already turned into a concrete key: the DAC consumes
// the walk's traversed chain instead of re-resolving the key component by
// component. Callers that resolved a user path must prefer these
// *Resolved variants; the plain forms remain for callers holding only a
// user path.
func CheckReadAccessResolved(ctx context.Context, repo *meta.RepoMetadata, cleanPath string, traversed []string) error {
	return checkPathAccessResolved(ctx, repo, cleanPath, traversed, accessRead)
}

// CheckWriteAccessResolved is CheckWriteAccess for an already-resolved
// concrete key (see CheckReadAccessResolved).
func CheckWriteAccessResolved(ctx context.Context, repo *meta.RepoMetadata, cleanPath string, traversed []string) error {
	return checkPathAccessResolved(ctx, repo, cleanPath, traversed, accessWrite)
}

// CheckListDirAccessResolved is CheckListDirAccess for an already-resolved
// concrete directory key (see CheckReadAccessResolved).
func CheckListDirAccessResolved(ctx context.Context, repo *meta.RepoMetadata, dirPath string, traversed []string) error {
	if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
		return err
	}
	attrs, err := lookupNode(repo, dirPath)
	if err != nil {
		return err
	}
	if !attrs.IsDir {
		return syscall.ENOTDIR
	}
	return checkAccess(IdentityFromContext(ctx), attrs, accessRead|accessExec)
}

// CheckParentWriteResolved is CheckParentWrite for an already-resolved
// concrete key (see CheckReadAccessResolved). The parent is a directory
// the resolution walk descended into, so its execute bit is covered by
// the traversed chain and only the write check needs the parent node.
func CheckParentWriteResolved(ctx context.Context, repo *meta.RepoMetadata, targetPath string, traversed []string) error {
	parent := ParentPath(targetPath)
	if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
		return err
	}
	attrs, err := lookupNode(repo, parent)
	if err != nil {
		return err
	}
	if !attrs.IsDir {
		return syscall.ENOTDIR
	}
	return checkAccess(IdentityFromContext(ctx), attrs, accessWrite|accessExec)
}

// CheckWalk verifies execute permission on the directories a user path's
// resolution walk descends into, in walk order (root first). It resolves
// first, then checks: symlink components are followed before checking
// ancestors, so POSIX traversal permission applies to the directories
// actually walked. The final component is left unresolved so that
// lstat/readlink-style operations check permission to reach the link
// itself, not its target.
func CheckWalk(ctx context.Context, repo *meta.RepoMetadata, targetPath string) error {
	_, traversed, err := resolvePathTracked(repo, targetPath, false)
	if err != nil {
		return err
	}
	return CheckWalkResolved(ctx, repo, traversed)
}

// CheckTraverse is the deprecated spelling of CheckWalk, kept so existing
// callers (including out-of-package storage verbs) keep compiling.
func CheckTraverse(ctx context.Context, repo *meta.RepoMetadata, targetPath string) error {
	return CheckWalk(ctx, repo, targetPath)
}

// CheckWalkResolved verifies execute permission on the directories a
// resolution walk actually descended into, in walk order (root first).
// Operations that resolved a user path with ResolveAccessPath must consume
// the returned traversed list through this check: re-resolving only the
// concrete key would miss the ancestors of an absolute link's own parent
// chain (a 0700 directory containing "link -> /pub/x" must not leak
// through the link).
func CheckWalkResolved(ctx context.Context, repo *meta.RepoMetadata, traversed []string) error {
	id := IdentityFromContext(ctx)
	checked := make(map[string]struct{}, len(traversed))
	for _, dir := range traversed {
		if err := checkDirExec(id, repo, checked, dir); err != nil {
			return err
		}
	}
	return nil
}

// CheckTraversal is the deprecated spelling of CheckWalkResolved, kept so
// existing callers (including out-of-package storage verbs) keep compiling.
func CheckTraversal(ctx context.Context, repo *meta.RepoMetadata, traversed []string) error {
	return CheckWalkResolved(ctx, repo, traversed)
}

func checkDirExec(id Identity, repo *meta.RepoMetadata, checked map[string]struct{}, dirPath string) error {
	if _, done := checked[dirPath]; done {
		return nil
	}
	checked[dirPath] = struct{}{}
	attrs, err := lookupNode(repo, dirPath)
	if err != nil {
		return err
	}
	if !attrs.IsDir {
		return syscall.ENOTDIR
	}
	return checkAccess(id, attrs, accessExec)
}

func CheckParentWrite(ctx context.Context, repo *meta.RepoMetadata, targetPath string) error {
	parent := ParentPath(targetPath)
	if err := CheckWalk(ctx, repo, parent); err != nil {
		return err
	}
	attrs, err := lookupNode(repo, parent)
	if err != nil {
		return err
	}
	if !attrs.IsDir {
		return syscall.ENOTDIR
	}
	return checkAccess(IdentityFromContext(ctx), attrs, accessWrite|accessExec)
}

func CanChmod(ctx context.Context, entry *EntryInfo) error {
	id := IdentityFromContext(ctx)
	if id.Admin || id.UID == entry.UID {
		return nil
	}
	return syscall.EPERM
}

func SanitizeChmodMode(ctx context.Context, entry *EntryInfo, mode uint32) uint32 {
	mode &= 0o7777
	id := IdentityFromContext(ctx)
	if id.Admin {
		return mode
	}
	if mode&0o2000 != 0 && !identityInGroup(id, entry.GID) {
		mode &^= 0o2000
	}
	return mode
}

func SanitizeWrittenFileMode(mode uint32) uint32 {
	return mode &^ 0o6000
}

// SanitizeWrittenFileModeForContext clears setuid+setgid on data writes
// for unprivileged callers (decision 1A). Admin is the CAP_FSETID
// equivalent and keeps the bits.
func SanitizeWrittenFileModeForContext(ctx context.Context, mode uint32) uint32 {
	if IdentityFromContext(ctx).Admin {
		return mode
	}
	return mode &^ 0o6000
}

// CanChown enforces POSIX chown(2): only root may move a file between
// owners; the file's owner may change the group to any group they belong
// to (Linux allows the owner the chgrp right, and a no-op uid value).
// The all-ones value is the chown(2) "leave unchanged" sentinel.
func CanChown(ctx context.Context, entry *EntryInfo, uid, gid uint32) error {
	const keepOwner = ^uint32(0)
	id := normalizeIdentity(IdentityFromContext(ctx))
	if id.Admin {
		return nil
	}
	if id.UID != entry.UID {
		return syscall.EPERM
	}
	if uid != keepOwner && uid != entry.UID {
		return syscall.EPERM
	}
	if gid != keepOwner && gid != entry.GID && !identityInGroup(id, gid) {
		return syscall.EPERM
	}
	return nil
}

// CanSetTimes gates utimensat-style timestamp updates. Owners and admins
// may set anything; a caller with only write permission may set the
// current time or omit a field (POSIX UTIME_NOW/UTIME_OMIT). Because the
// kernel resolves UTIME_NOW server-side, "current" is recognized within a
// small tolerance instead of by an unavailable flag.
func CanSetTimes(ctx context.Context, entry *EntryInfo) error {
	id := IdentityFromContext(ctx)
	if id.Admin || id.UID == entry.UID {
		return nil
	}
	return checkAccess(id, nodeAttrsFromEntry(entry), accessWrite)
}

// CanSetTimesValues is the value-aware form of CanSetTimes: nil pointers
// denote UTIME_OMIT. Non-owners with write permission may only omit or
// supply "now".
func CanSetTimesValues(ctx context.Context, entry *EntryInfo, atime, mtime *time.Time, now int64) error {
	id := IdentityFromContext(ctx)
	if id.Admin || id.UID == entry.UID {
		return nil
	}
	if err := checkAccess(id, nodeAttrsFromEntry(entry), accessWrite); err != nil {
		return err
	}
	for _, stamp := range []*time.Time{atime, mtime} {
		if stamp == nil {
			continue
		}
		if !isNowish(stamp.Unix(), now) {
			return syscall.EPERM
		}
	}
	return nil
}

// isNowish reports whether a requested timestamp is within the tolerance
// the kernel's UTIME_NOW resolution can produce (request handling latency
// plus coarse clocks).
func isNowish(requested, now int64) bool {
	diff := requested - now
	if diff < 0 {
		diff = -diff
	}
	return diff <= 2
}

func CanAccessEntry(id Identity, entry *EntryInfo, need int) error {
	return checkAccess(id, nodeAttrsFromEntry(entry), need)
}

func ApplyParentInheritance(repo *meta.RepoMetadata, targetPath string, isDir bool, mode, uid, gid uint32) (uint32, uint32, uint32) {
	parent := ParentPath(targetPath)
	attrs, err := lookupNode(repo, parent)
	if err != nil || !attrs.IsDir {
		return mode, uid, gid
	}
	if attrs.Mode&0o2000 != 0 {
		gid = attrs.GID
		if isDir {
			mode |= 0o2000
		}
	}
	return mode, uid, gid
}

func CheckStickyDelete(ctx context.Context, repo *meta.RepoMetadata, parentPath, targetPath string) error {
	parent, err := lookupNode(repo, parentPath)
	if err != nil {
		return err
	}
	if parent.Mode&0o1000 == 0 {
		return nil
	}
	id := IdentityFromContext(ctx)
	if id.Admin || id.UID == parent.UID {
		return nil
	}
	target, err := lookupNode(repo, targetPath)
	if err != nil {
		return err
	}
	if id.UID == target.UID {
		return nil
	}
	return syscall.EPERM
}

func TouchDirectory(repo *meta.RepoMetadata, dirPath string, now int64) {
	if repo == nil {
		return
	}
	if dirPath == "" {
		repo.Root.ModifiedAt = now
		repo.Root.ChangedAt = now
		return
	}
	if dir := repo.GetDirectory(dirPath); dir != nil {
		dir.ModifiedAt = now
		dir.ChangedAt = now
		repo.WriteDirDirect(dirPath, *dir)
	}
}

func TouchParentDirectory(repo *meta.RepoMetadata, targetPath string, now int64) {
	TouchDirectory(repo, ParentPath(targetPath), now)
}

func checkPathAccess(ctx context.Context, repo *meta.RepoMetadata, targetPath string, need int) error {
	if err := CheckWalk(ctx, repo, targetPath); err != nil {
		return err
	}
	attrs, err := lookupNode(repo, targetPath)
	if err != nil {
		return err
	}
	return checkAccess(IdentityFromContext(ctx), attrs, need)
}

// checkPathAccessResolved is checkPathAccess without the second
// resolution: cleanPath is a concrete key and traversed is the chain the
// original walk descended, which covers every ancestor of the key (in
// walk order) plus the symlink chains a re-resolution of the key alone
// would miss.
func checkPathAccessResolved(ctx context.Context, repo *meta.RepoMetadata, cleanPath string, traversed []string, need int) error {
	if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
		return err
	}
	attrs, err := lookupNode(repo, cleanPath)
	if err != nil {
		return err
	}
	return checkAccess(IdentityFromContext(ctx), attrs, need)
}

func lookupNode(repo *meta.RepoMetadata, targetPath string) (nodeAttrs, error) {
	clean := normalizeStoredPath(targetPath)
	if clean == "" {
		return nodeAttrs{Path: "", Mode: repo.Root.Mode, UID: repo.Root.UID, GID: repo.Root.GID, IsDir: true}, nil
	}
	if file := repo.FindFile(clean); file != nil {
		kind := meta.NodeKindFile
		if file.Symlink != "" {
			kind = meta.NodeKindSymlink
		}
		return nodeAttrs{Path: clean, Mode: file.Mode, UID: file.UID, GID: file.GID, Kind: kind}, nil
	}
	if dir := repo.GetDirectory(clean); dir != nil {
		return nodeAttrs{Path: clean, Mode: dir.Mode, UID: dir.UID, GID: dir.GID, IsDir: true}, nil
	}
	return nodeAttrs{}, syscall.ENOENT
}

func checkAccess(id Identity, attrs nodeAttrs, need int) error {
	id = normalizeIdentity(id)
	if id.Admin {
		if need&accessExec != 0 && !attrs.IsDir && attrs.Mode&0o111 == 0 {
			return syscall.EACCES
		}
		return nil
	}
	perm := permissionBits(id, attrs)
	if perm&need == need {
		return nil
	}
	return syscall.EACCES
}

func permissionBits(id Identity, attrs nodeAttrs) int {
	bits := int(attrs.Mode & 0o777)
	switch {
	case id.UID == attrs.UID:
		return (bits >> 6) & 0x7
	case identityInGroup(id, attrs.GID):
		return (bits >> 3) & 0x7
	default:
		return bits & 0x7
	}
}

func identityInGroup(id Identity, gid uint32) bool {
	if id.GID == gid {
		return true
	}
	for _, current := range id.Groups {
		if current == gid {
			return true
		}
	}
	return false
}

func nodeAttrsFromEntry(entry *EntryInfo) nodeAttrs {
	return nodeAttrs{Path: entry.Path, Mode: entry.Mode, UID: entry.UID, GID: entry.GID, IsDir: entry.IsDir, Kind: entry.Kind}
}

// normalizeIdentity canonicalizes group membership only. The Admin bit is
// decided where an identity is *asserted* (WithIdentity), never here: a
// uid-0 value that merely flowed through a check must not self-promote.
//
// The fast path matters: every DAC check re-normalizes the context
// identity, and WithIdentity already stores a normalized one, so the hot
// path must not clone+sort the group slice per check.
func normalizeIdentity(id Identity) Identity {
	if identityGroupsNormalized(id) {
		return id
	}
	id.Groups = uniqueGIDs(append([]uint32(nil), id.Groups...))
	if id.GID != 0 {
		id.Groups = uniqueGIDs(append(id.Groups, id.GID))
	}
	return id
}

// identityGroupsNormalized reports whether Groups already has exactly the
// shape normalizeIdentity produces: sorted, deduplicated, and (when the
// primary GID is nonzero) containing it.
func identityGroupsNormalized(id Identity) bool {
	if !slices.IsSorted(id.Groups) {
		return false
	}
	for i := 1; i < len(id.Groups); i++ {
		if id.Groups[i] == id.Groups[i-1] {
			return false
		}
	}
	if id.GID != 0 && !slices.Contains(id.Groups, id.GID) {
		return false
	}
	return true
}

func uniqueGIDs(groups []uint32) []uint32 {
	if len(groups) == 0 {
		return nil
	}
	sorted := slices.Clone(groups)
	slices.Sort(sorted)
	return slices.Compact(sorted)
}

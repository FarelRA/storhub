package fs

import (
	"context"
	"os"
	"path"
	"slices"
	"strings"
	"syscall"

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
	accessRead                      = 0x4
	accessWrite                     = 0x2
	accessExec                      = 0x1
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
	return context.WithValue(ctx, identityContextKey, id)
}

func IdentityFromContext(ctx context.Context) Identity {
	if ctx != nil {
		if id, ok := ctx.Value(identityContextKey).(Identity); ok {
			return normalizeIdentity(id)
		}
	}
	// Fail closed: an absent identity means the local process operating its
	// own repository — never an anonymous superuser. Multi-user surfaces
	// (FUSE, REST) must attach the real caller via WithIdentity; only a
	// process that genuinely runs as root normalizes to Admin.
	return normalizeIdentity(Identity{UID: uint32(os.Getuid()), GID: uint32(os.Getgid())})
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

func CheckExecAccess(ctx context.Context, repo *meta.RepoMetadata, filePath string) error {
	return checkPathAccess(ctx, repo, filePath, accessExec)
}

func CheckListDirAccess(ctx context.Context, repo *meta.RepoMetadata, dirPath string) error {
	if err := CheckTraverse(ctx, repo, dirPath); err != nil {
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

func CheckTraverse(ctx context.Context, repo *meta.RepoMetadata, targetPath string) error {
	id := IdentityFromContext(ctx)
	// Symlink components are followed before checking ancestors: POSIX
	// traversal permission applies to the directories actually walked.
	// The final component is left unresolved so that lstat/readlink-style
	// operations check permission to reach the link itself, not its target.
	resolved, err := ResolvePath(repo, targetPath, false)
	if err != nil {
		return err
	}
	for _, ancestor := range ancestorPaths(resolved) {
		attrs, err := lookupNode(repo, ancestor)
		if err != nil {
			return err
		}
		if !attrs.IsDir {
			return syscall.ENOTDIR
		}
		if err := checkAccess(id, attrs, accessExec); err != nil {
			return err
		}
	}
	return nil
}

func CheckParentWrite(ctx context.Context, repo *meta.RepoMetadata, targetPath string) error {
	parent := ParentPath(targetPath)
	if err := CheckTraverse(ctx, repo, parent); err != nil {
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

func CanChown(ctx context.Context) error {
	if IdentityFromContext(ctx).Admin {
		return nil
	}
	return syscall.EPERM
}

func CanSetTimes(ctx context.Context, entry *EntryInfo) error {
	id := IdentityFromContext(ctx)
	if id.Admin || id.UID == entry.UID {
		return nil
	}
	return checkAccess(id, nodeAttrsFromEntry(entry), accessWrite)
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
		repo.Dirs[dirPath] = *dir
	}
}

func TouchParentDirectory(repo *meta.RepoMetadata, targetPath string, now int64) {
	TouchDirectory(repo, ParentPath(targetPath), now)
}

func checkPathAccess(ctx context.Context, repo *meta.RepoMetadata, targetPath string, need int) error {
	if err := CheckTraverse(ctx, repo, targetPath); err != nil {
		return err
	}
	attrs, err := lookupNode(repo, targetPath)
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

func ancestorPaths(targetPath string) []string {
	clean := normalizeStoredPath(targetPath)
	if clean == "" {
		return []string{""}
	}
	parts := strings.Split(clean, "/")
	paths := []string{""}
	current := ""
	for i := 0; i < len(parts)-1; i++ {
		current = path.Join(current, parts[i])
		paths = append(paths, current)
	}
	return paths
}

func nodeAttrsFromEntry(entry *EntryInfo) nodeAttrs {
	return nodeAttrs{Path: entry.Path, Mode: entry.Mode, UID: entry.UID, GID: entry.GID, IsDir: entry.IsDir, Kind: entry.Kind}
}

func normalizeIdentity(id Identity) Identity {
	id.Groups = uniqueGIDs(append([]uint32(nil), id.Groups...))
	if id.GID != 0 {
		id.Groups = uniqueGIDs(append(id.Groups, id.GID))
	}
	if id.UID == 0 {
		id.Admin = true
	}
	return id
}

func uniqueGIDs(groups []uint32) []uint32 {
	if len(groups) == 0 {
		return nil
	}
	sorted := slices.Clone(groups)
	slices.Sort(sorted)
	return slices.Compact(sorted)
}

func currentIdentity() Identity {
	id := Identity{UID: uint32(os.Geteuid()), GID: uint32(os.Getegid())}
	if groups, err := os.Getgroups(); err == nil {
		id.Groups = make([]uint32, 0, len(groups))
		for _, group := range groups {
			if group < 0 {
				continue
			}
			id.Groups = append(id.Groups, uint32(group))
		}
	}
	return normalizeIdentity(id)
}

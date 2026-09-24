package rest

import (
	"context"
	shfs "github.com/FarelRA/storhub/internal/fs"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	storage "github.com/FarelRA/storhub/internal/storage"
	"io"
	"path"
	"strings"
)

// Compile-time proof that authorizedClient implements the FULL Client
// interface: a Client method added without a corresponding gate here fails
// the build instead of silently turning project routes into an
// unauthenticated pass-through (see clientFor, which fails closed at
// runtime).
var _ Client = (*authorizedClient)(nil)

type authorizedClient struct {
	base      Client
	principal *restPrincipal
}

func (c *authorizedClient) CreateFileContext(ctx context.Context, project, filePath string) (*metadata.FileMeta, error) {
	if err := c.requireCreate(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.CreateFileContext(ctx, project, filePath)
}
func (c *authorizedClient) MkdirContext(ctx context.Context, project, dirPath string) error {
	if err := c.requireCreate(ctx, project, dirPath); err != nil {
		return err
	}
	return c.base.MkdirContext(ctx, project, dirPath)
}
func (c *authorizedClient) DeleteFileContext(ctx context.Context, project, filePath string, opts ...shfs.MutateOption) error {
	if err := c.requireParentWrite(ctx, project, filePath); err != nil {
		return err
	}
	return c.base.DeleteFileContext(ctx, project, filePath, opts...)
}
func (c *authorizedClient) RmdirContext(ctx context.Context, project, dirPath string, opts ...shfs.MutateOption) error {
	if err := c.requireParentWrite(ctx, project, dirPath); err != nil {
		return err
	}
	if err := c.requireTraverse(ctx, project, dirPath); err != nil {
		return err
	}
	return c.base.RmdirContext(ctx, project, dirPath, opts...)
}
func (c *authorizedClient) RenameContext(ctx context.Context, project, oldPath, newPath string, opts ...shfs.MutateOption) error {
	if err := c.requireParentWrite(ctx, project, oldPath); err != nil {
		return err
	}
	if err := c.requireParentWrite(ctx, project, newPath); err != nil {
		return err
	}
	if err := c.requireTraverse(ctx, project, oldPath); err != nil {
		return err
	}
	return c.base.RenameContext(ctx, project, oldPath, newPath, opts...)
}
func (c *authorizedClient) CopyContext(ctx context.Context, project, srcPath, dstPath string) error {
	if err := c.requireTraverse(ctx, project, srcPath); err != nil {
		return err
	}
	entry, err := c.base.StatPathContext(ctx, project, srcPath)
	if err != nil {
		return err
	}
	if entry.IsDir {
		if !c.hasPerm(entry, permRead|permExec) {
			return errForbidden("permission denied")
		}
	} else {
		if !c.hasPerm(entry, permRead) {
			return errForbidden("permission denied")
		}
	}
	if err := c.requireCreate(ctx, project, dstPath); err != nil {
		return err
	}
	return c.base.CopyContext(ctx, project, srcPath, dstPath)
}

// CloneRange gates like CopyContext (read on the source, create on the
// destination) and passes the revision CAS options through to the core,
// which enforces them inside its transaction. DAC stays enforced twice:
// here for the REST principal, inside for the storage identity.
func (c *authorizedClient) CloneRange(ctx context.Context, project, src string, srcOff int64, dst string, dstOff int64, length int64, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := c.requireTraverse(ctx, project, src); err != nil {
		return nil, err
	}
	entry, err := c.base.StatPathContext(ctx, project, src)
	if err != nil {
		return nil, err
	}
	if entry.IsDir {
		if !c.hasPerm(entry, permRead|permExec) {
			return nil, errForbidden("permission denied")
		}
	} else {
		if !c.hasPerm(entry, permRead) {
			return nil, errForbidden("permission denied")
		}
	}
	if err := c.requireCreate(ctx, project, dst); err != nil {
		return nil, err
	}
	return c.base.CloneRange(ctx, project, src, srcOff, dst, dstOff, length, opts...)
}
func (c *authorizedClient) TruncateFileContext(ctx context.Context, project, filePath string, size int64, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := c.requireNodeWrite(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.TruncateFileContext(ctx, project, filePath, size, opts...)
}
func (c *authorizedClient) AppendFileContext(ctx context.Context, project, filePath string, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := c.requireNodeWrite(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.AppendFileContext(ctx, project, filePath, data, opts...)
}
func (c *authorizedClient) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := c.requireNodeWrite(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.WriteFileAtContext(ctx, project, filePath, offset, data, opts...)
}
func (c *authorizedClient) PatchFileContext(ctx context.Context, project, filePath string, offset, deleteSize int64, edit []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := c.requireNodeWrite(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.PatchFileContext(ctx, project, filePath, offset, deleteSize, edit, opts...)
}
func (c *authorizedClient) ReplaceFileFromReaderContext(ctx context.Context, project, filePath string, body io.Reader, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := c.requireNodeWrite(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.ReplaceFileFromReaderContext(ctx, project, filePath, body, opts...)
}
func (c *authorizedClient) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error) {
	if err := c.requireNodeRead(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.ReadFileAtContext(ctx, project, filePath, offset, length)
}
func (c *authorizedClient) StatPathContext(ctx context.Context, project, targetPath string) (*shfs.EntryInfo, error) {
	if err := c.requireTraverse(ctx, project, targetPath); err != nil {
		return nil, err
	}
	entry, err := c.base.StatPathContext(ctx, project, targetPath)
	if err != nil {
		return nil, err
	}
	if !c.canReadMetadata(entry) {
		return nil, errForbidden("permission denied")
	}
	return entry, nil
}
func (c *authorizedClient) ReadDirContext(ctx context.Context, project, dirPath string) ([]shfs.DirEntry, error) {
	// Stat-then-traverse order: a missing path answers 404 even to
	// callers who could not traverse its parents (existence oracle),
	// unlike StatPathContext which checks traverse first. Kept:
	// directory enumeration already reveals child names by design
	// (names, not bytes), so the oracle adds no new signal.
	entry, err := c.base.StatPathContext(ctx, project, dirPath)
	if err != nil {
		return nil, err
	}
	if err := c.requireTraverse(ctx, project, dirPath); err != nil {
		return nil, err
	}
	if !c.hasPerm(entry, permRead|permExec) {
		return nil, errForbidden("permission denied")
	}
	return c.base.ReadDirContext(ctx, project, dirPath)
}
func (c *authorizedClient) StatFSContext(ctx context.Context, project string) (*shfs.FSStats, error) {
	entry, err := c.base.StatPathContext(ctx, project, "")
	if err != nil {
		return nil, err
	}
	if !c.canReadMetadata(entry) {
		return nil, errForbidden("permission denied")
	}
	return c.base.StatFSContext(ctx, project)
}
func (c *authorizedClient) SymlinkContext(ctx context.Context, project, target, linkPath string) (*metadata.FileMeta, error) {
	if err := c.requireCreate(ctx, project, linkPath); err != nil {
		return nil, err
	}
	return c.base.SymlinkContext(ctx, project, target, linkPath)
}
func (c *authorizedClient) ReadlinkContext(ctx context.Context, project, linkPath string) (string, error) {
	if err := c.requireTraverse(ctx, project, linkPath); err != nil {
		return "", err
	}
	entry, err := c.base.StatPathContext(ctx, project, linkPath)
	if err != nil {
		return "", err
	}
	if !c.canReadMetadata(entry) {
		return "", errForbidden("permission denied")
	}
	return c.base.ReadlinkContext(ctx, project, linkPath)
}
func (c *authorizedClient) LinkContext(ctx context.Context, project, existingPath, newPath string) (*metadata.FileMeta, error) {
	if err := c.requireNodeRead(ctx, project, existingPath); err != nil {
		return nil, err
	}
	if err := c.requireCreate(ctx, project, newPath); err != nil {
		return nil, err
	}
	return c.base.LinkContext(ctx, project, existingPath, newPath)
}
func (c *authorizedClient) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) error {
	entry, err := c.base.StatPathContext(ctx, project, targetPath)
	if err != nil {
		return err
	}
	if !c.principal.Admin && c.principal.UID != entry.UID {
		return errForbidden("permission denied")
	}
	return c.base.ChmodContext(ctx, project, targetPath, mode)
}
func (c *authorizedClient) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) error {
	// Same rule as FUSE and CLI (shfs.CanChown): admins may move
	// anything, and the file owner may change group within their own
	// membership (UID kept or equal). Anything else fails closed here,
	// never wider than CanChown; the backend re-enforces the same rule
	// under the request identity.
	entry, err := c.base.StatPathContext(ctx, project, targetPath)
	if err != nil {
		return err
	}
	idCtx := shfs.WithIdentity(ctx, shfs.Identity{
		UID:    c.principal.UID,
		GID:    c.principal.PrimaryGID,
		Groups: append([]uint32(nil), c.principal.Groups...),
		Admin:  c.principal.Admin,
		Umask:  defaultRESTUmask,
	})
	if err := shfs.CanChown(idCtx, entry, uid, gid); err != nil {
		return errForbidden("permission denied")
	}
	return c.base.ChownContext(ctx, project, targetPath, uid, gid)
}
func (c *authorizedClient) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) error {
	entry, err := c.base.StatPathContext(ctx, project, targetPath)
	if err != nil {
		return err
	}
	if !c.principal.Admin && c.principal.UID != entry.UID && !c.hasPerm(entry, permWrite) {
		return errForbidden("permission denied")
	}
	return c.base.ChtimesContext(ctx, project, targetPath, atime, mtime)
}
func (c *authorizedClient) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte, mode ...shfs.XAttrMode) error {
	if err := c.requireNodeWrite(ctx, project, targetPath); err != nil {
		return err
	}
	return c.base.SetXAttrContext(ctx, project, targetPath, attr, data, mode...)
}
func (c *authorizedClient) GetXAttrContext(ctx context.Context, project, targetPath, attr string) ([]byte, error) {
	if err := c.requireNodeRead(ctx, project, targetPath); err != nil {
		return nil, err
	}
	return c.base.GetXAttrContext(ctx, project, targetPath, attr)
}
func (c *authorizedClient) ListXAttrContext(ctx context.Context, project, targetPath string) ([]string, error) {
	if err := c.requireNodeRead(ctx, project, targetPath); err != nil {
		return nil, err
	}
	return c.base.ListXAttrContext(ctx, project, targetPath)
}
func (c *authorizedClient) RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) error {
	if err := c.requireNodeWrite(ctx, project, targetPath); err != nil {
		return err
	}
	return c.base.RemoveXAttrContext(ctx, project, targetPath, attr)
}

// RevisionContext exposes the repo-level metadata revision; gated like
// other project-wide metadata (root readability).
func (c *authorizedClient) RevisionContext(ctx context.Context, project string) (string, error) {
	entry, err := c.base.StatPathContext(ctx, project, "")
	if err != nil {
		return "", err
	}
	if !c.canReadMetadata(entry) {
		return "", errForbidden("permission denied")
	}
	if rs, ok := c.base.(shfs.RevisionSource); ok {
		return rs.RevisionContext(ctx, project)
	}
	return "", errForbidden("revision preconditions unsupported by backend")
}

func (c *authorizedClient) ListMetadataRevisionsContext(ctx context.Context, project string) ([]metadata.MetadataRevision, error) {
	entry, err := c.base.StatPathContext(ctx, project, "")
	if err != nil {
		return nil, err
	}
	if !c.canReadMetadata(entry) {
		return nil, errForbidden("permission denied")
	}
	return c.base.ListMetadataRevisionsContext(ctx, project)
}
func (c *authorizedClient) RollbackMetadataContext(ctx context.Context, project, commitSHA string) error {
	if !c.principal.Admin {
		return errForbidden("permission denied")
	}
	return c.base.RollbackMetadataContext(ctx, project, commitSHA)
}
func (c *authorizedClient) RevertPathContext(ctx context.Context, project, path, commitSHA string) error {
	if !c.principal.Admin {
		return errForbidden("permission denied")
	}
	return c.base.RevertPathContext(ctx, project, path, commitSHA)
}
func (c *authorizedClient) PruneContext(ctx context.Context, project, scope string, keep int, dryRun bool) (*storage.PruneResult, error) {
	if !c.principal.Admin {
		return nil, errForbidden("permission denied")
	}
	return c.base.PruneContext(ctx, project, scope, keep, dryRun)
}

func (c *authorizedClient) DegradedProjects() ([]string, error) {
	if !c.principal.Admin {
		return nil, errForbidden("permission denied")
	}
	return c.base.DegradedProjects()
}

func (c *authorizedClient) ReEnableProject(project string) error {
	if !c.principal.Admin {
		return errForbidden("permission denied")
	}
	return c.base.ReEnableProject(project)
}

func (c *authorizedClient) PressureSnapshot() (storage.PressureSnapshot, error) {
	if !c.principal.Admin {
		return storage.PressureSnapshot{}, errForbidden("permission denied")
	}
	return c.base.PressureSnapshot()
}

func (c *authorizedClient) PressureFailureStreak(project string) (uint64, error) {
	// Admin-gated like the sibling pressure accessors above: hub-wide
	// operability counters stay on the operator surface. handleStatus
	// already requires admin transitively through DegradedProjects and
	// PressureSnapshot; the explicit gates keep the lane closed even for
	// future direct callers.
	if !c.principal.Admin {
		return 0, errForbidden("permission denied")
	}
	return c.base.PressureFailureStreak(project)
}

func (c *authorizedClient) PressurePendingDepth(project string) (int, error) {
	if !c.principal.Admin {
		return 0, errForbidden("permission denied")
	}
	return c.base.PressurePendingDepth(project)
}
func (c *authorizedClient) DeleteProjectContext(ctx context.Context, project string) error {
	if !c.principal.Admin {
		return errForbidden("permission denied")
	}
	return c.base.DeleteProjectContext(ctx, project)
}

// DrainProjectContext forwards the sync drain to the base client. The
// mutation endpoints already gated the write itself; the drain only waits
// for that committed work to land, so no additional check applies.
func (c *authorizedClient) DrainProjectContext(ctx context.Context, project string) error {
	return c.base.DrainProjectContext(ctx, project)
}

const (
	permRead  = 4
	permWrite = 2
	permExec  = 1
)

func (c *authorizedClient) requireCreate(ctx context.Context, project, filePath string) error {
	return c.requireParentWrite(ctx, project, filePath)
}

func (c *authorizedClient) requireNodeRead(ctx context.Context, project, filePath string) error {
	if err := c.requireTraverse(ctx, project, filePath); err != nil {
		return err
	}
	entry, err := c.base.StatPathContext(ctx, project, filePath)
	if err != nil {
		return err
	}
	if !c.hasPerm(entry, permRead) {
		return errForbidden("permission denied")
	}
	return nil
}

func (c *authorizedClient) requireNodeWrite(ctx context.Context, project, filePath string) error {
	if err := c.requireTraverse(ctx, project, filePath); err != nil {
		return err
	}
	entry, err := c.base.StatPathContext(ctx, project, filePath)
	if err != nil {
		return err
	}
	if !c.hasPerm(entry, permWrite) {
		return errForbidden("permission denied")
	}
	return nil
}

func (c *authorizedClient) requireParentWrite(ctx context.Context, project, filePath string) error {
	parent := parentRESTPath(filePath)
	if err := c.requireTraverse(ctx, project, parent); err != nil {
		return err
	}
	entry, err := c.base.StatPathContext(ctx, project, parent)
	if err != nil {
		return err
	}
	if !entry.IsDir || !c.hasPerm(entry, permWrite|permExec) {
		return errForbidden("permission denied")
	}
	return nil
}

func (c *authorizedClient) requireTraverse(ctx context.Context, project, targetPath string) error {
	if c.principal.Admin {
		return nil
	}
	for _, dir := range ancestorRESTPaths(targetPath) {
		entry, err := c.base.StatPathContext(ctx, project, dir)
		if err != nil {
			return err
		}
		if !entry.IsDir || !c.hasPerm(entry, permExec) {
			return errForbidden("permission denied")
		}
	}
	return nil
}

func (c *authorizedClient) canReadMetadata(entry *shfs.EntryInfo) bool {
	if c.principal.Admin {
		return true
	}
	if entry.IsDir {
		return c.hasPerm(entry, permExec)
	}
	return c.hasPerm(entry, permRead)
}

func (c *authorizedClient) hasPerm(entry *shfs.EntryInfo, need int) bool {
	if c.principal.Admin {
		return true
	}
	bits := int(entry.Mode & 0o777)
	shift := 0
	if c.principal.UID == entry.UID {
		shift = 6
	} else if c.inGroup(entry.GID) {
		shift = 3
	}
	perm := (bits >> shift) & 0x7
	return perm&need == need
}

func (c *authorizedClient) inGroup(gid uint32) bool {
	if gid == c.principal.PrimaryGID {
		return true
	}
	for _, group := range c.principal.Groups {
		if group == gid {
			return true
		}
	}
	return false
}

func ancestorRESTPaths(targetPath string) []string {
	clean := strings.Trim(path.Clean("/"+strings.TrimSpace(targetPath)), "/")
	if clean == "" {
		return []string{""}
	}
	parts := strings.Split(clean, "/")
	paths := []string{""}
	current := ""
	for i := 0; i < len(parts)-1; i++ {
		if current == "" {
			current = parts[i]
		} else {
			current += "/" + parts[i]
		}
		paths = append(paths, current)
	}
	return paths
}

func parentRESTPath(targetPath string) string {
	clean := strings.Trim(path.Clean("/"+strings.TrimSpace(targetPath)), "/")
	if clean == "" {
		return ""
	}
	parent := path.Dir(clean)
	if parent == "." {
		return ""
	}
	return parent
}

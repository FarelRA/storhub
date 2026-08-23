package rest

import (
	"context"
	"strings"
	"testing"
)

// authzCase pins one cell of the principal×permission×operation matrix.
type authzCase struct {
	name string
	// principal under test
	principal restPrincipal
	// operation against the seeded tree
	op      func(c *authorizedClient) error
	allowed bool
}

func newMatrixTree(t *testing.T) *authorizedClientFactory {
	t.Helper()
	client := newFakeRESTClient()
	mk := func(op func() error) {
		t.Helper()
		if err := op(); err != nil {
			t.Fatalf("seed tree: %v", err)
		}
	}
	setModeOwner := func(project, path string, mode uint32, uid, gid uint32) {
		// Seeding goes through the authorization layer itself (as admin) so
		// mode/owner data lands in the fake's storage rather than in a
		// throwaway stat copy.
		admin := &authorizedClient{base: client, principal: &restPrincipal{Kind: "user", Username: "root", Admin: true}}
		mk(func() error { return admin.ChmodContext(context.Background(), project, path, mode) })
		mk(func() error { return admin.ChownContext(context.Background(), project, path, uid, gid) })
	}
	mk(func() error { return client.MkdirContext(context.Background(), "demo", "private") })
	// private is 0700 owned by alice: others cannot even traverse it.
	setModeOwner("demo", "private", 0o700, 1001, 2001)
	mk(func() error {
		_, err := client.CreateFileContext(context.Background(), "demo", "private/secret.txt")
		return err
	})
	setModeOwner("demo", "private/secret.txt", 0o600, 1001, 2001)

	mk(func() error { return client.MkdirContext(context.Background(), "demo", "team") })
	setModeOwner("demo", "team", 0o770, 1001, 2001)
	mk(func() error {
		_, err := client.CreateFileContext(context.Background(), "demo", "team/plan.txt")
		return err
	})
	setModeOwner("demo", "team/plan.txt", 0o640, 1001, 2001)

	return &authorizedClientFactory{client: client}
}

type authorizedClientFactory struct{ client *fakeRESTClient }

func (f *authorizedClientFactory) as(p restPrincipal) *authorizedClient {
	return &authorizedClient{base: f.client, principal: &p}
}

func TestAuthorizationMatrix(t *testing.T) {
	f := newMatrixTree(t)
	owner := restPrincipal{Kind: "user", Username: "alice", UID: 1001, PrimaryGID: 2001}
	groupMember := restPrincipal{Kind: "user", Username: "bob", UID: 1002, PrimaryGID: 3000, Groups: []uint32{2001}}
	outsider := restPrincipal{Kind: "user", Username: "carol", UID: 1003, PrimaryGID: 3000}
	admin := restPrincipal{Kind: "user", Username: "root", UID: 0, PrimaryGID: 0, Admin: true}

	readSecret := func(path string) func(*authorizedClient) error {
		return func(c *authorizedClient) error {
			_, err := c.ReadFileAtContext(context.Background(), "demo", path, 0, 16)
			return err
		}
	}
	appendPlan := func(c *authorizedClient) error {
		_, err := c.AppendFileContext(context.Background(), "demo", "team/plan.txt", []byte("x"))
		return err
	}
	createInTeam := func(c *authorizedClient) error {
		_, err := c.CreateFileContext(context.Background(), "demo", "team/new.txt")
		return err
	}
	chownPlanToSelf := func(uid uint32) func(*authorizedClient) error {
		return func(c *authorizedClient) error {
			return c.ChownContext(context.Background(), "demo", "team/plan.txt", uid, 2001)
		}
	}
	listRevisions := func(c *authorizedClient) error {
		_, err := c.ListMetadataRevisionsContext(context.Background(), "demo")
		return err
	}
	purge := func(c *authorizedClient) error {
		_, err := c.PurgeUntrackedContext(context.Background(), "demo")
		return err
	}
	deleteProject := func(c *authorizedClient) error {
		return c.DeleteProjectContext(context.Background(), "demo")
	}

	cases := []authzCase{
		// Reads inside a 0700 directory: only its owner and admin pass.
		{name: "owner reads private file", principal: owner, op: readSecret("private/secret.txt"), allowed: true},
		{name: "group member denied by traverse into private", principal: groupMember, op: readSecret("private/secret.txt"), allowed: false},
		{name: "other denied by traverse into private", principal: outsider, op: readSecret("private/secret.txt"), allowed: false},
		{name: "admin bypasses traverse and mode bits", principal: admin, op: readSecret("private/secret.txt"), allowed: true},

		// Group-bit selection on team content (0640 / dir 0770 gid 2001).
		{name: "owner has write on group file", principal: owner, op: appendPlan, allowed: true},
		{name: "group member reads group file", principal: groupMember, op: readSecret("team/plan.txt"), allowed: true},
		{name: "group member lacks write bit (0640)", principal: groupMember, op: appendPlan, allowed: false},
		{name: "other cannot read group-only file", principal: outsider, op: readSecret("team/plan.txt"), allowed: false},
		{name: "group member creates via parent wx bits", principal: groupMember, op: createInTeam, allowed: true},
		{name: "other cannot create in team dir", principal: outsider, op: createInTeam, allowed: false},

		// chown policy: a privileged operation, restricted to admins at the
		// REST layer regardless of file ownership (unlike chmod).
		{name: "owner cannot chown via REST", principal: owner, op: chownPlanToSelf(1009), allowed: false},
		{name: "non-owner group member cannot chown", principal: groupMember, op: chownPlanToSelf(1009), allowed: false},
		{name: "admin chowns arbitrary file", principal: admin, op: chownPlanToSelf(1009), allowed: true},

		// Metadata and destructive operations are gated above DAC.
		// Revision listing is gated by metadata READ access (root here is
		// world-readable), unlike rollback/purge/deletion which are
		// admin-only regardless of DAC.
		{name: "admin lists revisions", principal: admin, op: listRevisions, allowed: true},
		{name: "reader lists revisions on readable root", principal: outsider, op: listRevisions, allowed: true},
		{name: "admin purges", principal: admin, op: purge, allowed: true},
		{name: "non-admin denied purge", principal: outsider, op: purge, allowed: false},
		{name: "admin deletes project", principal: admin, op: deleteProject, allowed: true},
		{name: "non-admin denied project deletion", principal: owner, op: deleteProject, allowed: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.op(f.as(tc.principal))
			if tc.allowed && err != nil {
				t.Fatalf("expected allowed, got: %v", err)
			}
			if !tc.allowed && (err == nil || !strings.Contains(err.Error(), "denied") && !strings.Contains(err.Error(), "permission")) {
				t.Fatalf("expected denial, got: %v", err)
			}
		})
	}
}

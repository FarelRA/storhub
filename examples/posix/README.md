# POSIX Example

This example focuses on StorHub's POSIX-like metadata features.

What it teaches:

- chmod, chown, and chtimes flows
- xattr set/get/list/remove flows
- symlink creation and readlink
- hardlink creation and shared inode-style metadata

Why this example exists:

- POSIX metadata is one of the more unusual StorHub features
- it helps explain what the mounted FUSE layer relies on underneath

How to run it:

```bash
GITHUB_TOKEN=your_token go run ./examples/posix
```

Public APIs highlighted:

- `(*StorHub).ChmodContext`
- `(*StorHub).ChownContext`
- `(*StorHub).ChtimesContext`
- `(*StorHub).SetXAttrContext`
- `(*StorHub).GetXAttrContext`
- `(*StorHub).ListXAttrContext`
- `(*StorHub).RemoveXAttrContext`
- `(*StorHub).SymlinkContext`
- `(*StorHub).ReadlinkContext`
- `(*StorHub).LinkContext`

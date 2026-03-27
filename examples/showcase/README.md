# Showcase Example

This is the broadest example in the repository.

What it teaches:

- how to create clients with `storhub.NewStorHub`, `storhub.NewStorHubWithConfig`, and `storhub.NewStorHubWithContext`
- how the main storage, filesystem, POSIX, revision, integrity, and FUSE APIs fit together in one workflow
- what a realistic end-to-end StorHub session looks like

Why this example exists:

- to show the whole public API surface in one place
- to help you understand how the smaller focused examples relate to each other
- to act like a guided tour before you specialize on one feature area

How to run it:

```bash
GITHUB_TOKEN=your_token go run ./examples/showcase
```

Optional environment variables:

- `STORHUB_DELETE_PROJECT=1` to delete the created project at the end
- `STORHUB_DELETE_RELEASE_TAG=<tag>` to delete a specific release during the maintenance phase

What it does:

1. prints config defaults and constructor usage
2. creates directories and managed files
3. uploads, replaces, patches, downloads, and verifies files
4. uses filesystem-style APIs like create, write, append, truncate, rename, stat, and readdir
5. uses POSIX-style APIs like chmod, chown, chtimes, symlinks, hardlinks, and xattrs
6. previews FUSE setup through the public `fuse` package
7. lists revisions, rolls back metadata, and runs cleanup operations

If you want one specific slice only, use the focused examples in sibling directories.

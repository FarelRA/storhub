# Revisions Example

This example focuses on metadata history and maintenance operations.

What it teaches:

- how metadata revisions accumulate
- how to list revision history
- how to roll back to an older metadata commit
- how purge and cleanup interact with project maintenance

Why this example exists:

- StorHub's metadata-history model is a key part of its design
- maintenance APIs are important, but they should be learned separately from basic file operations

How to run it:

```bash
GITHUB_TOKEN=your_token go run ./examples/revisions
```

Optional environment variables:

- `STORHUB_DELETE_PROJECT=1` to remove the project after the demo
- `STORHUB_DELETE_RELEASE_TAG=<tag>` to delete a release tag explicitly

Public APIs highlighted:

- `(*StorHub).ListMetadataRevisions`
- `(*StorHub).RollbackMetadata`
- `(*StorHub).PurgeUntracked`
- `(*StorHub).CleanupProject`
- `(*StorHub).DeleteRelease`
- `(*StorHub).DeleteProject`

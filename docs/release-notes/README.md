# Release notes

One file per tag, named after the tag: `docs/release-notes/v0.3.2.md` for `v0.3.2`.
The file holds the release body as published — no title line, `gh` takes the title
from the tag.

`.github/workflows/release.yml` reads the file out of the checked-out tag. **Commit
the notes before creating the tag.** A file merged afterwards is not in the tagged
tree and the release will not see it; the workflow then falls back to
`--generate-notes` and publishes the raw commit list instead.

So the order is: open the notes in a PR, merge it, then tag the merge commit.
If a release does go out with generated notes, fix it with
`gh release edit <tag> --notes-file docs/release-notes/<tag>.md` and commit the file
for the next one.

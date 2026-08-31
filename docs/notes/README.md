# Notes

Long-form documents rendered in the dashboard's **Docs** view, and readable here
on GitHub.

They are about evaluation *methodology* and about demonstrating this toolkit,
which is why they are not on the [API reference
site](https://text2sql-eval-toolkit.readthedocs.io/): that site is generated
from docstrings and describes the code.

| Document | What it is |
|---|---|
| [state-of-the-art.md](state-of-the-art.md) | How text-to-SQL systems are measured, what each family of metrics tells you, and where each one lies to you |
| [worked-examples.md](worked-examples.md) | The recurring shapes where two metrics disagree, what each one means, and what to do about it |
| [demo-walkthrough.md](demo-walkthrough.md) | Six screens, in order, and the point each one makes |

## Adding one

Write a `.md` file here. That is the whole process — there is no registry and no
code change. The dashboard reads the file's first `#` heading as its title and
its first paragraph as the summary in the list, and it becomes addressable at
`/docs/<filename-without-the-extension>`.

Two constraints, both enforced by the server:

- The filename must be a plain stem — letters, digits, dots, dashes and
  underscores, starting with a letter or digit. Anything else is not
  addressable, so it is skipped rather than listed with a link that 404s.
- Raw HTML in the Markdown is sanitised before rendering. Scripts, iframes,
  forms, styles and event handlers are stripped, so a document can display text
  and nothing more.

## Where they are, and are not

These files are **not packaged**. `docs/` is absent from both the wheel and the
sdist, deliberately, and CI checks that it stays that way — the notes live in
the repository and are public here, rather than shipping to PyPI.

The consequence is that the docs view is empty on a `pip install`. That is the
intended behaviour, and the view explains it and links here. A checkout has
them, and `deploy/Dockerfile` copies them into the deployment image, so the two
places that matter both do.

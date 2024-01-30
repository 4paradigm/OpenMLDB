# Contributing
Please refer to [Contribution Guideline](https://github.com/4paradigm/OpenMLDB/blob/main/CONTRIBUTING.md)

## Pull Request (PR) Guidelines

When submitting a PR, please pay attention to the following points:
- PR Title: Please adhere to the [commit format](https://github.com/4paradigm/rfcs/blob/main/style-guide/commit-convention.md#conventional-commits-reference) for the PR title. **Note that this refers to the PR title, not the commits within the PR**.
```{note}
If the title does not meet the standard, `pr-linter / pr-name-lint (pull_request)` will fail with a status of `x`.
```
- PR Checks: There are various checks in a PR, and only `codecov/patch` and `codecov/project` may not pass. Other checks should pass. If other checks do not pass and you cannot fix them or believe they should not be fixed, you can leave a comment in the PR.

- PR Description: Please explain the intent of the PR in the first comment of the PR. We provide a PR comment template, and while you are not required to follow it, ensure that there is sufficient explanation.

- PR Files Changed: Pay attention to the `files changed` in the PR. Do not include code changes outside the scope of the PR intent. You can generally eliminate unnecessary diffs by using `git merge origin/main` followed by `git push` to the PR branch. If you need assistance, leave a comment in the PR.
```{note}
If you are not modifying the code based on the main branch, when the PR intends to merge into the main branch, the `files changed` will include unnecessary code. For example, if the main branch is at commit 10, and you start from commit 9 of the old main, add new_commit1, and then add new_commit2 on top of new_commit1, you actually only want to submit new_commit2, but the PR will include new_commit1 and new_commit2.
In this case, just use `git merge origin/main` and `git push` to the PR branch to only include the changes.
```
```{seealso}
If you want the branch code to be cleaner, you can avoid using `git merge` and use `git rebase -i origin/main` instead. It will add your changes one by one on top of the main branch. However, it will change the commit history, and you need `git push -f` to override the branch.
```

## Compilation Guidelines

For compilation details, refer to the [Compilation Documentation](../deploy/compile.md). To avoid the impact of operating systems and tool versions, we recommend compiling OpenMLDB in a compilation image. Since compiling the entire OpenMLDB requires significant space, we recommend using `OPENMLDB_BUILD_TARGET` to specify only the parts you need.
# 代码贡献指南
详见我们 GitHub 上的代码贡献指南： [Contribution Guideline](https://github.com/4paradigm/OpenMLDB/blob/main/CONTRIBUTING.md)

## Pull Request(PR)须知

提交PR时请注意以下几点：
- PR标题，请遵守[commit格式](https://github.com/4paradigm/rfcs/blob/main/style-guide/commit-convention.md#conventional-commits-reference)。**注意是PR标题，而不是PR中的commits**。
```{note}
如果标题不符合标准，`pr-linter / pr-name-lint (pull_request)`将会失败，状态为`x`。
```
- PR checks，PR中有很多checks，只有`codecov/patch`和`codecov/project`可以不通过，其他checks都应该通过。如果其他checks不通过，而你无法修复或认为不应修复，可以在PR中留下评论。

- PR说明，请在PR的第一个comment中说明PR的意图。我们提供了PR comment模板，你可以不遵守该模板，但也请保证有足够的解释。

- PR files changed，请注意pr的`files changed`。不要包含PR意图以外的代码改动。基本可以通过`git merge origin/main`再`git push`到PR分支，来消除多余diff。如果你需要帮助，请在PR中评论。
```{note}
如果你不是在main分支的基础上修改代码，那么PR希望合入main分支时，`files changed`就会包含多余代码。比如，main分支已经是commit10，你从old main的commit9开始，增加了new_commit1，在new_commit1的基础上，增加new_commit2，实际上你只是想提交new_commit2，但PR中会包含new_commit1和new_commit2。
这种情况，只需要`git merge origin/main`，再`git push`到PR分支，就可以只有改动部分。
```
```{seealso}
如果你希望分支的代码更加clean，可以不用`git merge`，而是使用`git rebase -i origin/main`，它会将你的更改在main分支的基础上逐一增加。但它会改变commit，你需要`git push -f`来覆盖分支。
```

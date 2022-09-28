# 代码贡献
Please refer to [Contribution Guideline](https://github.com/4paradigm/OpenMLDB/blob/main/CONTRIBUTING.md)

## Pull Request须知

提交pr时请注意以下几点：
- pr标题，请遵守[commit格式](https://github.com/4paradigm/rfcs/blob/main/style-guide/commit-convention.md#conventional-commits-reference)。**注意是pr标题，而不是pr中的commits**。
```{note}
如果标题不符合标准，`pr-linter / pr-name-lint (pull_request)`将会失败，状态为`x`。
```
- pr checks，pr中有很多checks，只有`codecov/patch`和`codecov/project`可以不通过，其他checks都应该通过。如果其他checks不通过，而你无法修复或认为不应修复，可以在pr中留下评论。

- pr说明，请在pr的第一个comment中说明pr的意图。我们提供了pr comment模板，你可以不遵守该模板，但也请保证有足够的解释。

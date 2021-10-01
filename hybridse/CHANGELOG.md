# Changelog

## [Unreleased]
### Feature
- Support aggregation function, e.g. `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, over the whole table [#219](https://github.com/4paradigm/OpenMLDB/issues/219)
- Enhance plan optimization on `GROUP` and `FILTER` op [#350](https://github.com/4paradigm/OpenMLDB/pull/350)
- Refactor status code and status macro. Save first message (root message) in `status.msg`. [#430](https://github.com/4paradigm/OpenMLDB/issues/430)

### Bug Fix
- Fix plan error triggered by optimize the same plan node repeatedly. [#437](https://github.com/4paradigm/OpenMLDB/issues/437)


## [0.2.3] - 2021-08-31
### Feature
- Support parameterized query under BatchMode [#262](https://github.com/4paradigm/OpenMLDB/issues/262)

### SQL Syntax
- `nvl` & `nvl2`:  [#238](https://github.com/4paradigm/OpenMLDB/issues/238)
- bitwise operators: `&`, `|`, `^`, `~` [#244](https://github.com/4paradigm/OpenMLDB/pull/244)
- between predicate: [#277](https://github.com/4paradigm/OpenMLDB/pull/277)


## [0.2.1] - 2021-08-06
### Feature
+ Add `VARCHAR` Type [#237](https://github.com/4paradigm/OpenMLDB/issues/237)

### Bug Fix
- Fix invalid back qoute identifier name [#263](https://github.com/4paradigm/OpenMLDB/issues/263). 
  
### Note: 
`OPTIONS` can't write as multiple path style (e.g a.b) now

## [0.2.0] - 2021-07-16
### SQL Syntax

Changed
- `lag` method: [#163](https://github.com/4paradigm/HybridSE/issues/163)

Removed
- `lead` function: [#163](https://github.com/4paradigm/HybridSE/issues/163)

## [0.1.5] - 2021-07-14

### Features

+ refactor front-end using [zetasql](https://github.com/jingchen2222/zetasql)
+ better code style and comment

### SQL Syntax

Changed
- `table options` syntax: [#103](https://github.com/4paradigm/HybridSE/issues/103)
- `lead` method: [#136](https://github.com/4paradigm/HybridSE/pull/136)

Removed
- `||` and `&&` as logical operator: [#99](https://github.com/4paradigm/HybridSE/issues/99)
- `at` function: [#136](https://github.com/4paradigm/HybridSE/pull/136)

[Unreleased]: https://github.com/4paradigm/OpenMLDB/compare/hybridse-v0.2.3...HEAD
[0.2.3]: https://github.com/4paradigm/OpenMLDB/releases/tag/hybridse-v0.2.3
[0.2.1]: https://github.com/4paradigm/HybridSE/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/4paradigm/HybridSE/compare/v0.1.5...v0.2.0
[0.1.5]: https://github.com/4paradigm/HybridSE/compare/v0.1.4...v0.1.5

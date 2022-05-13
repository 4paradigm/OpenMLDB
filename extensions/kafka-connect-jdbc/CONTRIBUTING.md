# Contributing

In order for us to consider merging a contribution, you will need to sign our
**C**ontributor **L**icense **A**greement.

> The purpose of a CLA is to ensure that the guardian of a project's outputs has the necessary ownership or grants of rights over all contributions to allow them to distribute under the chosen licence.
> [Wikipedia](http://en.wikipedia.org/wiki/Contributor_License_Agreement)

You can read and sign our full Contributor License Agreement [here](http://clabot.confluent.io/cla).

## Reporting Bugs and Issues

Report bugs and issues by creating a new GitHub issue. Prior to creating an issue, please search
through existing issues so that you are not creating duplicate ones. If a pull request exists that
corresponds to the issue, mention this pull request on the GitHub issue.

## Guidelines for Contributing Code, Examples, Documentation

Code changes are submitted via a pull request (PR). When submitting a PR use the following
guidelines:

* Follow the style guide below
* Add/update documentation appropriately for the change you are making.
* Non-trivial changes should include unit tests covering the new functionality and potentially integration tests.
* Bug fixes should include unit tests and/or integration tests proving the issue is fixed.
* Try to keep pull requests short and submit separate ones for unrelated features.
* Keep formatting changes in separate commits to make code reviews easier and distinguish them from actual code changes.

### Code Style
This connector is using a coding style that generally follows the [Google Java coding standard guide](https://google.github.io/styleguide/javaguide.html).

Some conventions worth mentioning are:

* Indentation (single tab) is 2 spaces.
* All import statements are listed explicitly. The wildcard (*) is not used in imports.
* Imports are groups as follows:
```
    import all packages not listed below (all other imports)
    <blank line>
    import all javax.* packages
    import all java.* packages
    <blank line>
    import all io.confluent.* packages
    <blank line>
    import static packages
```
* Javadoc is highly recommended and often required during reviews in interfaces and public or protected classes and methods.

### Titles and changelogs

The title of a pull request is used as an entry on the release notes (aka changelogs) of the
connector in every release.

For this reason, please use a brief but descriptive title for your pull request. If GitHub shortens
your pull request title when you issue the pull request adding the excessive part to the pull
request description, make sure that you correct the title before or after you issue the pull
request.

If the fix is a minor fix you are encouraged to use the tag `MINOR:` followed by your pull request
title. You may link the corresponding issue to the description of your pull request but adding it to
the title will not be useful during changelog generation.

When reverting a previous commit, use the prefix `Revert ` on the pull request title (automatically
added by GitHub when a pull request is created to revert an existing commit).

### Tests
Every pull request should contain a sufficient amount of tests that assess your suggested code
changes. It’s highly recommended that you also check the code coverage of the production code you
are adding to make sure that your changes are covered sufficiently by the test code.

### Description
Including a good description when you issue your pull requests helps significantly with reviews.
Feel free to follow the template that is when issuing a pull request and mention how your changes
are tested.

### Backporting Commits
If your code changes are essentially bug fixes that make sense to backport to existing releases make sure to target the earliest release branch (e.g. 2.0.x) that should contain your changes. When selecting the release branch you should also consider how easy it will be to resolve any conflicts in newer release branches, including the `master` branch.

## Github Workflow

1. Fork the connector repository into your GitHub account: https://github.com/confluentinc/kafka-connect-jdbc/fork

2. Clone your fork of the GitHub repository, replacing `<username>` with your GitHub username.

    Use ssh (recommended):

    ```bash
    git clone git@github.com:<username>/kafka-connect-jdbc.git
    ```

    Or https:

    ```bash
    git clone https://github.com/<username>/kafka-connect-jdbc.git
    ```

3. Add a remote to keep up with upstream changes.

    ```bash
    git remote add upstream https://github.com/confluentinc/kafka-connect-jdbc.git
    ```

    If you already have a copy, fetch upstream changes.

    ```bash
    git fetch upstream
    ```

    or

    ```bash
    git remote update
    ```

4. Create a feature branch to work in.

    ```bash
    git checkout -b feature-xyz upstream/master
    ```

5. Work in your feature branch.

    ```bash
    git commit -a --verbose
    ```

6. Periodically rebase your changes

    ```bash
    git pull --rebase
    ```

7. When done, combine ("squash") related commits into a single one

    ```bash
    git rebase -i upstream/master
    ```

    This will open your editor and allow you to re-order commits and merge them:
    - Re-order the lines to change commit order (to the extent possible without creating conflicts)
    - Prefix commits using `s` (squash) or `f` (fixup) to merge extraneous commits.

8. Submit a pull-request

    ```bash
    git push origin feature-xyz
    ```

    Go to your fork main page

    ```bash
    https://github.com/<username>/kafka-connect-jdbc.git
    ```

    If you recently pushed your changes GitHub will automatically pop up a `Compare & pull request`
    button for any branches you recently pushed to. If you click that button it will automatically
    offer you to submit your pull-request to the `confluentinc` connector repository.

    - Give your pull-request a meaningful title as described [above](#titles-and-changelogs).
    - In the description, explain your changes and the problem they are solving.

9. Addressing code review comments

    Repeat steps 5. through 7. to address any code review comments and rebase your changes if necessary.

    Push your updated changes to update the pull request

    ```bash
    git push origin [--force] feature-xyz
    ```

    `--force` may be necessary to overwrite your existing pull request in case your
    commit history was changed when performing the rebase.

    Note: Be careful when using `--force` since you may lose data if you are not careful.

    ```bash
    git push origin --force feature-xyz
    ```

## Useful Resources for Developers

1. Connector Developer Guide: https://docs.confluent.io/platform/current/connect/devguide.html
2. A Guide to the Confluent Verified Integrations Program: https://www.confluent.io/blog/guide-to-confluent-verified-integrations-program/
3. Verification Guide for Confluent Platform Integrations: https://cdn.confluent.io/wp-content/uploads/Verification-Guide-Confluent-Platform-Connectors-Integrations.pdf
4. From Zero to Hero with Kafka Connect: https://www.confluent.io/kafka-summit-lon19/from-zero-to-hero-with-kafka-connect/
5. 4 Steps to Creating Apache Kafka Connectors with the Kafka Connect API: https://www.confluent.io/blog/create-dynamic-kafka-connect-source-connectors/
6. How to Write a Connector for Kafka Connect – Deep Dive into Configuration Handling: https://www.confluent.io/blog/write-a-kafka-connect-connector-with-configuration-handling/

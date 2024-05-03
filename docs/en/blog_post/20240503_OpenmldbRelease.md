# OpenMLDB v0.9.0 Release: Major Upgrade in SQL Capabilities Covering the Entire Feature Servicing Process

OpenMLDB has just released a new version v0.9.0, including SQL syntax extensions, MySQL protocol compatibility, TiDB storage support, online feature computation, feature signatures, and more. Among these, the most noteworthy features are the MySQL protocol and ANSI SQL compatibility, along with the extended SQL syntax capabilities.

Firstly, MySQL protocol compatibility allows OpenMLDB users to access OpenMLDB clusters using any MySQL client, not limited to GUI applications like NaviCat or Sequal Ace but also Java JDBC MySQL Driver, Python SQLAlchemy, Go MySQL Driver, and various programming language SDKs. For more information, you can refer to “[**Ultra High-Performance Database OpenM(ysq)LDB: Seamless Compatibility with MySQL Protocol and Multi-Language MySQL Client**](https://openmldb.medium.com/ultra-high-performance-database-openm-ysq-ldb-seamless-compatibility-with-mysql-protocol-and-d3f60210feea)”.

Secondly, the new version significantly expands SQL capabilities, especially implementing OpenMLDB’s unique request mode and stored procedure execution within standard SQL syntax. Compared to traditional SQL databases, OpenMLDB covers the entire machine learning process, including offline and online modes. In online mode, users can input sample data, and get feature results through SQL feature extraction. On the contrary, in the past, we needed to deploy SQL as a stored procedure through the `Deploy` command and then perform online feature computation through SDKs or HTTP interfaces. The new version adds `SELECT CONFIG` and `CALL` statements, allowing users to directly specify request mode and sample data in SQL to compute feature results, as shown below:

```
-- Execute online request mode query for action (10, "foo", timestamp(4000))
SELECT id, count(val) over (partition by id order by ts rows between 10 preceding and current row)
FROM t1
CONFIG (execute_mode = 'online', values = (10, "foo", timestamp(4000)))
```
You can also use the ANSI SQL `CALL` statement to invoke stored procedures with sample rows as parameters, as shown below:

```
-- Execute online request mode query for action (10, "foo", timestamp(4000))
DEPLOY window_features SELECT id, count(val) over (partition by id order by ts rows between 10 preceding and current row)
FROM t1;
    
CALL window_features(10, "foo", timestamp(4000))
```
For detailed release notes, please refer to: [https://github.com/4paradigm/OpenMLDB/releases/tag/v0.9.0](https://github.com/4paradigm/OpenMLDB/releases/tag/v0.9.0)

Please feel free to download and explore the latest release. Your feedback is highly valued and appreciated. We encourage you to share your thoughts and suggestions to help us improve and enhance the platform. Thank you for your support!

## Release Date

April 25, 2024

## Release Note

[https://github.com/4paradigm/OpenMLDB/releases/tag/v0.9.0](https://github.com/4paradigm/OpenMLDB/releases/tag/v0.9.0)

## Highlighted Features

* Added support for the latest version of SQLAlchemy 2, seamlessly integrating with popular Python frameworks such as Pandas and Numpy.

* Expanded support for more data backends, integrating TiDB’s distributed file storage capability with OpenMLDB’s high-performance in-memory feature computation capability.

* Enhanced ANSI SQL support, fixed `first_value` semantics, supported `MAP` type and feature signatures, and added offline mode support for `INSERT` statements.

* Added support for MySQL protocol, allowing access to OpenMLDB clusters using MySQL clients like NaviCat, Sequal Ace, and various MySQL SDKs for programming languages.

* Extended SQL syntax support, enabling online feature computation directly through `SELECT CONFIG` or `CALL` statements.

--------------------------------------------------------------------------------------------------------------

**For more information on OpenMLDB:**
* Official website: [https://openmldb.ai/](https://openmldb.ai/)
* GitHub: [https://github.com/4paradigm/OpenMLDB](https://github.com/4paradigm/OpenMLDB)
* Documentation: [https://openmldb.ai/docs/en/](https://openmldb.ai/docs/en/)
* Join us on [**Slack**](https://join.slack.com/t/openmldb/shared_invite/zt-ozu3llie-K~hn9Ss1GZcFW2~K_L5sMg)!

> _This post is a re-post from [OpenMLDB Blogs](https://openmldb.medium.com/)._

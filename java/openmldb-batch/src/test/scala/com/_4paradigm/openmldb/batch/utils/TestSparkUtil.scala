package com._4paradigm.openmldb.batch.utils

import com._4paradigm.hybridse.node.JoinType
import com._4paradigm.openmldb.batch.utils.SparkUtil.supportNativeLastJoin
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar.mock


class TestSparkUtil extends FunSuite{

  test("Test supportNativeLastJoin") {
    assert(supportNativeLastJoin(JoinType.kJoinTypeFull,true) == false)
    assert(supportNativeLastJoin(JoinType.kJoinTypeFull,false) == false)
    assert(supportNativeLastJoin(JoinType.kJoinTypeLast,false) == false)
  }

}


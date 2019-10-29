/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * emun.h
 *      
 * Author: chenjing
 * Date: 2019/10/29 
 *--------------------------------------------------------------------------
**/

#ifndef FESQL_EMUN_H
#define FESQL_EMUN_H
namespace fesql {
namespace node {


const std::string SPACE_ST = "+-";
const std::string SPACE_ED = "";
const std::string INDENT = "|\t";
enum SQLNodeType {
    kSelectStmt = 0,
    kExpr,
    kResTarget,
    kTable,
    kFunc,
    kWindowFunc,
    kWindowDef,
    kFrameBound,
    kFrames,
    kColumn,
    kConst,
    kLimit,
    kAll,
    kList,
    kOrderBy,

    kNull,
    kInt,
    kBigInt,
    kFloat,
    kDouble,
    kString,

    kDesc,
    kAsc,

    kFrameRange,
    kFrameRows,

    kPreceding,
    kFollowing,
    kCurrent,
    kUnknow
};
/**
 * Planner:
 *  basic class for plan
 *
 */
enum PlanType {
    kSelect,
    kProjectList,
    kProject,
    kScalarFunction,
    kOpExpr,
    kAggFunction,
    kAggWindowFunction,
    kUnknowPlan,
};
}
}
#endif //FESQL_EMUN_H

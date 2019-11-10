/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * sql_node.cc
 *
 * Author: chenjing
 * Date: 2019/10/11
 *--------------------------------------------------------------------------
 **/

#include "node/sql_node.h"
#include <numeric>
#include "glog/logging.h"
#include "node/node_manager.h"
namespace fesql {
namespace node {

void SQLNode::Print(std::ostream &output, const std::string &tab) const {
  output << tab << SPACE_ST << "node[" << NameOfSQLNodeType(type_) << "]";
}

void SQLNodeList::Print(std::ostream &output, const std::string &tab) const {
  if (0 == size_ || NULL == head_) {
    output << tab << "[]";
    return;
  }
  output << tab << "[\n";
  SQLLinkedNode *p = head_;
  const std::string space = tab + "\t";
  p->node_ptr_->Print(output, space);
  output << "\n";
  p = p->next_;
  while (NULL != p) {
    p->node_ptr_->Print(output, space);
    p = p->next_;
    output << "\n";
  }
  output << tab << "]";
}

void SQLNodeList::PushFront(SQLLinkedNode *linked_node_ptr) {
  linked_node_ptr->next_ = head_;
  head_ = linked_node_ptr;
  size_ += 1;
  if (NULL == tail_) {
    tail_ = head_;
  }
}
void SQLNodeList::AppendNodeList(SQLNodeList *node_list_ptr) {
  if (NULL == node_list_ptr) {
    return;
  }

  if (NULL == tail_) {
    head_ = node_list_ptr->head_;
    tail_ = head_;
    size_ = node_list_ptr->size_;
    return;
  }

  tail_->next_ = node_list_ptr->head_;
  tail_ = node_list_ptr->tail_;
  size_ += node_list_ptr->size_;
}

void ConstNode::Print(std::ostream &output, const std::string &org_tab) const {
  {
    SQLNode::Print(output, org_tab);
    output << "\n";
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << tab << SPACE_ST;
    switch (date_type_) {
      case kTypeInt32:
        output << "value: " << val_.vint;
        break;
      case kTypeInt64:
        output << "value: " << val_.vlong;
        break;
      case kTypeString:
        output << "value: " << val_.vstr;
        break;
      case kTypeFloat:
        output << "value: " << val_.vfloat;
        break;
      case kTypeDouble:
        output << "value: " << val_.vdouble;
        break;
      case kTypeNull:
        output << "value: null";
        break;
      default:
        output << "value: unknow";
    }
  }
}

void LimitNode::Print(std::ostream &output, const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  const std::string tab = org_tab + INDENT + SPACE_ED;
  output << "\n";
  PrintValue(output, tab, std::to_string(limit_cnt_), "limit_cnt", true);
}

void TableNode::Print(std::ostream &output, const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  const std::string tab = org_tab + INDENT + SPACE_ED;
  output << "\n";
  PrintValue(output, tab, org_table_name_, "table", false);
  output << "\n";
  PrintValue(output, tab, alias_table_name_, "alias", true);
}

void ColumnRefNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  const std::string tab = org_tab + INDENT + SPACE_ED;
  output << "\n";
  PrintValue(output, tab, relation_name_, "relation_name", false);
  output << "\n";
  PrintValue(output, tab, column_name_, "column_name", true);
}

void OrderByNode::Print(std::ostream &output,
                        const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  const std::string tab = org_tab + INDENT + SPACE_ED;

  output << "\n";
  PrintValue(output, tab, NameOfSQLNodeType(sort_type_), "sort_type", false);

  output << "\n";
  PrintSQLNode(output, tab, order_by_, "order_by", true);
}

void FrameNode::Print(std::ostream &output, const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  const std::string tab = org_tab + INDENT + SPACE_ED;

  output << "\n";
  PrintValue(output, tab, NameOfSQLNodeType(frame_type_), "type", false);

  output << "\n";
  if (NULL == start_) {
    PrintValue(output, tab, "UNBOUNDED", "start", false);
  } else {
    PrintSQLNode(output, tab, start_, "start", false);
  }

  output << "\n";
  if (NULL == end_) {
    PrintValue(output, tab, "UNBOUNDED", "end", true);
  } else {
    PrintSQLNode(output, tab, end_, "end", true);
  }
}

void FuncNode::Print(std::ostream &output, const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  output << "\n";
  const std::string tab = org_tab + INDENT + SPACE_ED;
  output << tab << "function_name: " << function_name_;
  output << "\n";
  PrintSQLVector(output, tab, args_, "args", false);
  output << "\n";
  PrintSQLNode(output, tab, over_, "over", true);
}

void WindowDefNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  const std::string tab = org_tab;
  output << "\n";
  PrintValue(output, tab, window_name_, "window_name", false);

  output << "\n";
  PrintSQLVector(output, tab, partition_list_ptr_, "partitions", false);

  output << "\n";
  PrintSQLVector(output, tab, order_list_ptr_, "orders", false);

  output << "\n";
  PrintSQLNode(output, tab, frame_ptr_, "frame", true);
}

void ResTarget::Print(std::ostream &output, const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  output << "\n";
  const std::string tab = org_tab + INDENT + SPACE_ED;
  PrintSQLNode(output, tab, val_, "val", false);
  output << "\n";
  PrintValue(output, tab, name_, "name", true);
}

void SelectStmt::Print(std::ostream &output, const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  const std::string tab = org_tab + INDENT + SPACE_ED;
  output << "\n";
  bool last_child = false;
  PrintSQLNode(output, tab, where_clause_ptr_, "where_clause", last_child);
  output << "\n";
  PrintSQLNode(output, tab, group_clause_ptr_, "group_clause", last_child);
  output << "\n";
  PrintSQLNode(output, tab, having_clause_ptr_, "haveing_clause", last_child);
  output << "\n";
  PrintSQLNode(output, tab, order_clause_ptr_, "order_clause", last_child);
  output << "\n";
  PrintSQLNode(output, tab, limit_ptr_, "limit", last_child);
  output << "\n";
  PrintSQLVector(output, tab, select_list_ptr_, "select_list", last_child);
  output << "\n";
  PrintSQLVector(output, tab, tableref_list_ptr_, "tableref_list", last_child);
  output << "\n";
  last_child = true;
  PrintSQLVector(output, tab, window_list_ptr_, "window_list", last_child);
}

/**
 * get the node type name
 * @param type
 * @param output
 */
std::string NameOfSQLNodeType(const SQLNodeType &type) {
  std::string output;
  switch (type) {
    case kSelectStmt:
      output = "SELECT";
      break;
    case kCreateStmt:
      output = "CREATE";
      break;
    case kName:
      output = "kName";
      break;
    case kResTarget:
      output = "kResTarget";
      break;
    case kTable:
      output = "kTable";
      break;
    case kColumnRef:
      output = "kColumnRef";
      break;
    case kColumnDesc:
      output = "kColumnDesc";
      break;
    case kColumnIndex:
      output = "kColumnIndex";
      break;
    case kExpr:
      output = "kExpr";
      break;
    case kFunc:
      output = "kFunc";
      break;
    case kWindowDef:
      output = "kWindowDef";
      break;
    case kFrames:
      output = "kFrame";
      break;
    case kFrameBound:
      output = "kBound";
      break;
    case kPreceding:
      output = "kPreceding";
      break;
    case kFollowing:
      output = "kFollowing";
      break;
    case kCurrent:
      output = "kCurrent";
      break;
    case kFrameRange:
      output = "kFrameRange";
      break;
    case kFrameRows:
      output = "kFrameRows";
      break;
    case kConst:
      output = "kConst";
      break;
    case kOrderBy:
      output = "kOrderBy";
      break;
    case kLimit:
      output = "kLimit";
      break;
    case kAll:
      output = "kAll";
      break;
    default:
      output = "unknown";
  }
  return output;
}

std::ostream &operator<<(std::ostream &output, const SQLNode &thiz) {
  thiz.Print(output, "");
  return output;
}

std::ostream &operator<<(std::ostream &output, const SQLNodeList &thiz) {
  thiz.Print(output, "");
  return output;
}

void FillSQLNodeList2NodeVector(
    SQLNodeList *node_list_ptr,
    std::vector<SQLNode *> &node_list  // NOLINT (runtime/references)
) {
  if (nullptr != node_list_ptr) {
    SQLLinkedNode *ptr = node_list_ptr->GetHead();
    while (nullptr != ptr && nullptr != ptr->node_ptr_) {
      node_list.push_back(ptr->node_ptr_);
      ptr = ptr->next_;
    }
  }
}

std::string WindowOfExpression(SQLNode *node_ptr) {
  switch (node_ptr->GetType()) {
    case kFunc: {
      FuncNode *func_node_ptr = dynamic_cast<FuncNode *>(node_ptr);
      if (nullptr != func_node_ptr->GetOver()) {
        return func_node_ptr->GetOver()->GetName();
      }

      if (func_node_ptr->GetArgs().empty()) {
        return "";
      }

      for (auto arg : func_node_ptr->GetArgs()) {
        std::string arg_w = WindowOfExpression(arg);
        if (false == arg_w.empty()) {
          return arg_w;
        }
      }

      return "";
    }
    default:
      return "";
  }
}

void PrintSQLNode(std::ostream &output, const std::string &org_tab,
                  SQLNode *node_ptr, const std::string &item_name,
                  bool last_child) {
  output << org_tab << SPACE_ST << item_name << ":";

  if (nullptr == node_ptr) {
    output << " null";
  } else if (last_child) {
    output << "\n";
    node_ptr->Print(output, org_tab + INDENT);
  } else {
    output << "\n";
    node_ptr->Print(output, org_tab + OR_INDENT);
  }
}

void PrintSQLVector(std::ostream &output, const std::string &tab,
                    NodePointVector vec, const std::string &vector_name,
                    bool last_item) {
  if (0 == vec.size()) {
    output << tab << SPACE_ST << vector_name << ": []";
    return;
  }
  output << tab << SPACE_ST << vector_name << "[list]: \n";
  const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
  int count = vec.size();
  int i = 0;
  for (i = 0; i < count - 1; ++i) {
    PrintSQLNode(output, space, vec[i], "" + std::to_string(i), false);
    output << "\n";
  }
  PrintSQLNode(output, space, vec[i], "" + std::to_string(i), true);
}

void PrintValue(std::ostream &output, const std::string &org_tab,
                const std::string &value, const std::string &item_name,
                bool last_child) {
  output << org_tab << SPACE_ST << item_name << ": " << value;
}

void CreateStmt::Print(std::ostream &output, const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  const std::string tab = org_tab + INDENT + SPACE_ED;
  output << "\n";
  PrintValue(output, tab, table_name_, "table", false);
  output << "\n";
  PrintValue(output, tab, std::to_string(op_if_not_exist_), "IF NOT EXIST",
             false);
  output << "\n";
  PrintSQLVector(output, tab, column_desc_list_, "column_desc_list_", true);
  output << "\n";
  //    if (nullptr != table_def_) {
  //        PrintValue(output, tab, table_def_->DebugString() , "table def",
  //        true);
  //    }
}

void ColumnDefNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  const std::string tab = org_tab + INDENT + SPACE_ED;
  output << "\n";
  PrintValue(output, tab, column_name_, "column_name", false);
  output << "\n";
  PrintValue(output, tab, DataTypeName(column_type_), "column_type", false);
  output << "\n";
  PrintValue(output, tab, std::to_string(op_not_null_), "NOT NULL", false);
}

void ColumnIndexNode::Print(std::ostream &output,
                            const std::string &org_tab) const {
  SQLNode::Print(output, org_tab);
  const std::string tab = org_tab + INDENT + SPACE_ED;
  output << "\n";
  std::string lastdata;
  lastdata = accumulate(key_.begin(), key_.end(), lastdata);
  PrintValue(output, tab, lastdata, "keys", false);
  output << "\n";
  PrintValue(output, tab, ts_, "ts_col", false);
  output << "\n";
  PrintValue(output, tab, std::to_string(ttl_), "ttl", false);
  output << "\n";
  PrintValue(output, tab, version_, "version_column", false);
  output << "\n";
  PrintValue(output, tab, std::to_string(version_count_), "version_count",
             true);
}
}  // namespace node
}  // namespace fesql

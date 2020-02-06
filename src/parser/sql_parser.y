%define api.pure full
%output  "sql_parser.gen.cc"
%defines "sql_parser.gen.h"
%define parse.error verbose
%locations
%lex-param   { yyscan_t scanner }
%parse-param { yyscan_t scanner }
%parse-param { ::fesql::node::NodePointVector &trees}
%parse-param { ::fesql::node::NodeManager *node_manager}
%parse-param { ::fesql::base::Status &status}

%{
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <utility>
#include "node/sql_node.h"
#include "node/node_manager.h"
#include "base/status.h"
#include "parser/sql_parser.gen.h"

extern int yylex(YYSTYPE* yylvalp, 
                 YYLTYPE* yyllocp, 
                 yyscan_t scanner);
void emit(const char *s, ...);
void yyerror(YYLTYPE* yyllocp, yyscan_t unused, ::fesql::node::NodePointVector &trees,
	::fesql::node::NodeManager *node_manager, ::fesql::base::Status &status, const char* msg) {
	status.code=::fesql::common::kSQLError;
	std::ostringstream s;
        s << "line: "<< yyllocp->last_line << ", column: "
       	<< yyllocp->first_column << ": " <<
       	msg;
	status.msg=(s.str());
}
%}

%code requires {
#include "node/sql_node.h"
#include "base/status.h"
#include <sstream>
#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif
}

%union {
	int intval;
	double floatval;
	char* strval;
	int subtok;
	bool flag;
	::fesql::node::SQLNode* node;
	::fesql::node::FnNode* fnnode;
	::fesql::node::ExprNode* expr;
	::fesql::node::DataType type;
	::fesql::node::FnNodeList* fnlist;
	::fesql::node::ExprListNode* exprlist;
	::fesql::node::SQLNodeList* list;
}

/* names and literal values */
%token <strval> NAME
%token <strval> STRING
%token <intval> INTNUM
%token <intval> DAYNUM
%token <intval> HOURNUM
%token <intval> MINUTENUM
%token <intval> SECONDNUM
%token <intval> BOOL
%token <floatval> APPROXNUM

/* user @abc names */

%token <strval> USERVAR
%token <strval> VARNAME

/* operators and precedence levels */

%right ASSIGN
%left OR
%left XOR
%left ANDOP
%nonassoc IN IS LIKE REGEXP
%left NOT '!'
%left BETWEEN
%left '<' '>' EQUALS LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%left '|'
%left '&'
%left <subtok> SHIFT /* << >> */
%left '+' '-'
%left '*' '/' '%' MOD
%left '^'
%nonassoc UMINUS

%token <intval> I32
%token <strval> NEWLINE
%token <intval> INDENT
%token <strval> DEF

%token ADD
%token ALL
%token ALTER
%token ANALYZE
%token AND
%token ANY
%token AS
%token ASC
%token AUTO_INCREMENT
%token BEFORE
%token BETWEEN
%token BIGINT
%token BINARY
%token BIT
%token BLOB
%token BOTH
%token BY
%token CALL
%token CASCADE
%token CASE
%token CHANGE
%token CHAR
%token CHECK
%token COLLATE
%token COLUMN
%token COMMENT
%token CONDITION
%token CONSTRAINT
%token CONTINUE
%token CONVERT
%token CREATE
%token CROSS
%token CURRENT
%token CURRENT_DATE
%token CURRENT_TIME
%token CURRENT_TIMESTAMP
%token CURRENT_USER
%token CURSOR
%token DATABASE
%token DATABASES
%token DATE
%token DATETIME
%token DAY_HOUR
%token DAY_MICROSECOND
%token DAY_MINUTE
%token DAY_SECOND
%token DECIMAL
%token DECLARE
%token DEFAULT
%token DELAYED
%token DELETE
%token DESC
%token DESCRIBE
%token DETERMINISTIC
%token DISTINCT
%token DISTINCTROW
%token DIV
%token DOUBLE
%token DROP
%token DUAL
%token EACH
%token ELSE
%token ELSEIF
%token ENCLOSED
%token END
%token FUNDEFEND
%token ENUM
%token ESCAPED
%token EXISTS
%token EXIT
%token EXPLAIN
%token FETCH
%token FLOAT
%token FOR
%token FORCE
%token FOREIGN
%token FOLLOWING
%token FROM
%token FULLTEXT
%token GRANT
%token GROUP
%token HAVING
%token HIGH_PRIORITY
%token HOUR_MICROSECOND
%token HOUR_MINUTE
%token HOUR_SECOND
%token IF
%token IGNORE
%token IN
%token INDEX
%token INFILE
%token INNER
%token INOUT
%token INSENSITIVE
%token INSERT
%token INT
%token INTEGER
%token INTERVAL
%token INTO
%token ITERATE
%token JOIN
%token KEY
%token KEYS
%token KILL
%token LEADING
%token LEAVE
%token LEFT
%token LIKE
%token LIMIT
%token LINES
%token LOAD
%token LOCALTIME
%token LOCALTIMESTAMP
%token LOCK
%token LONG
%token LONGBLOB
%token LONGTEXT
%token LOOP
%token LOW_PRIORITY
%token MATCH
%token MEDIUMBLOB
%token MEDIUMINT
%token MEDIUMTEXT
%token MINUTE_MICROSECOND
%token MINUTE_SECOND
%token MOD
%token MODIFIES
%token NATURAL
%token NOT
%token NO_WRITE_TO_BINLOG
%token NULLX
%token NUMBER
%token ON
%token ONDUPLICATE
%token OPTIMIZE
%token OPTION
%token OPTIONALLY
%token OR
%token ORDER
%token OUT
%token OUTER
%token OUTFILE
%token OVER
%token PARTITION
%token PRECISION
%token PRIMARY
%token PROCEDURE
%token PURGE
%token QUICK
%token RANGE
%token READ
%token READS
%token REAL
%token REFERENCES
%token REGEXP
%token RELEASE
%token RENAME
%token REPEAT
%token REPLACE
%token REQUIRE
%token RESTRICT
%token RETURN
%token REVOKE
%token PRECEDING
%token RIGHT
%token ROLLUP
%token ROW
%token ROWS
%token SCHEMA
%token SCHEMAS
%token SECOND_MICROSECOND
%token SELECT
%token SENSITIVE
%token SEPARATOR
%token SET
%token SHOW
%token SMALLINT
%token SOME
%token SONAME
%token SPATIAL
%token SPECIFIC
%token SQL
%token SQLEXCEPTION
%token SQLSTATE
%token SQLWARNING
%token SQL_BIG_RESULT
%token SQL_CALC_FOUND_ROWS
%token SQL_SMALL_RESULT
%token SSL
%token STRINGTYPE
%token STARTING
%token STRAIGHT_JOIN
%token TABLE
%token TABLES
%token TEMPORARY
%token TEXT
%token TERMINATED
%token THEN
%token TIME
%token TIMESTAMP
%token TINYBLOB
%token TINYINT
%token TINYTEXT
%token TO
%token TRAILING
%token TRIGGER
%token TS
%token TTL

%token UNDO
%token UNION
%token UNIQUE
%token UNLOCK
%token UNSIGNED
%token UPDATE
%token USAGE
%token USE
%token USING
%token UNBOUNDED
%token UTC_DATE
%token UTC_TIME
%token UTC_TIMESTAMP
%token VALUES
%token VARBINARY
%token VARCHAR
%token VARYING
%token VERSION
%token WINDOW
%token WHEN
%token WHERE
%token WHILE
%token WITH
%token WRITE
%token XOR
%token YEAR
%token YEAR_MONTH
%token ZEROFILL

 /* functions with special syntax */
%token FSUBSTRING
%token FTRIM
%token FDATE_ADD FDATE_SUB
%token FCOUNT

 /* udf */
%type <type> types
%type <fnnode> grammar line_list
			   fun_def_block fn_def_indent_op  func_stmt
               fn_def return_stmt assign_stmt para
               if_stmt elif_stmt else_stmt
%type<fnlist> plist stmt_block func_stmts

%type <expr> var expr primary_time column_ref call_expr expr_const frame_expr
 /* select stmt */
%type <node>  sql_stmt stmt select_stmt
              opt_all_clause
              table_factor table_reference
              projection

              opt_frame_clause frame_bound frame_extent
              window_definition window_specification over_clause
              limit_clause

 /* insert table */
%type<node> insert_stmt
%type<exprlist> insert_expr_list column_ref_list opt_partition_clause sort_clause opt_sort_clause
%type<expr> insert_expr

 /* create table */
%type <node>  create_stmt column_desc column_index_item column_index_key
%type <node>  cmd_stmt
%type <flag>  op_not_null op_if_not_exist
%type <list>  column_desc_list column_index_item_list

%type <list> opt_target_list
            select_projection_list expr_list
            table_references

            window_clause window_definition_list

%type <strval> relation_name relation_factor
               column_name
               function_name
               opt_existing_window_name
               NEWLINES
               database_name table_name group_name file_path

%type <intval> opt_window_exclusion_clause


%start grammar

%%
grammar :
        line_list
        ;

/**** function def ****/


line_list:
	fun_def_block {
            trees.push_back($1);
        }
        | sql_stmt {
            trees.push_back($1);
        }
        | line_list NEWLINES fun_def_block
        {
            trees.push_back($3);
        }
        | line_list NEWLINES sql_stmt
        {
        	trees.push_back($3);
        }
        | line_list NEWLINES {$$ = $1;}
        | NEWLINES line_list {$$ = $2;}
        ;
NEWLINES: NEWLINE {}
	| NEWLINES NEWLINE {}
fun_def_block : fn_def_indent_op NEWLINES stmt_block {
            $$ = node_manager->MakeFnListNode();
            ((::fesql::node::FnNodeList*)$$)->AddChild($1);
            for (auto item: ((::fesql::node::FnNodeList*)$3)->GetChildren()) {
                ((::fesql::node::FnNodeList*)$$)->AddChild(item);
            }
        }
        ;


fn_def_indent_op:
        fn_def {
            $$ = $1;
        }
        |INDENT fn_def {$$=$2; $$->indent=$1;}
        ;


stmt_block:
        func_stmts NEWLINES FUNDEFEND {
            emit("enter stmt block");
            $$ = $1;
        }
        ;
func_stmts:
        func_stmt {
			$$ = node_manager->MakeFnListNode();
            $$->AddChild($1);
        }
        |func_stmts NEWLINES func_stmt {
            emit("enter func stmts");
            $$ = $1;
            $$->AddChild($3);
        }
        ;
func_stmt:
         return_stmt {
            emit("enter return stmt");
            $1->indent = 0;
            $$ = $1;
         }
         |INDENT return_stmt
         {
            emit("INDENT enter return stmt");
            $2->indent = $1;
            $$ = $2;
         }
         |INDENT assign_stmt
         {
            emit("INDENT enter assign stmt");
            $2->indent = $1;
            $$ = $2;
         }
         |INDENT if_stmt
         {
         	emit("INDENT enter if stmt");
            $2->indent = $1;
            $$ = $2;
         }
         |INDENT elif_stmt
         {
         	emit("INDENT enter if stmt");
            $2->indent = $1;
            $$ = $2;
         }
         |INDENT else_stmt
         {
         	emit("INDENT enter else stmt");
            $2->indent = $1;
            $$ = $2;
         };

fn_def :
       DEF VARNAME'(' plist ')' ':' types {
            $$ = node_manager->MakeFnDefNode($2, $4, $7);
       };

assign_stmt: VARNAME ASSIGN expr {
            $$ = node_manager->MakeAssignNode($1, $3);
           };

return_stmt:
           RETURN expr {
            $$ = node_manager->MakeReturnStmtNode($2);
           };

if_stmt:
		IF expr {
			$$ = node_manager->MakeIfStmtNode($2);
		};
elif_stmt:
		ELSEIF expr {
			$$ = node_manager->MakeElifStmtNode($2);
		};
else_stmt:
		ELSE {
			$$ = node_manager->MakeElseStmtNode();
		}

types:  I32
        {
            $$ = ::fesql::node::kTypeInt32;
        }
        |INTEGER
        {
            $$ = ::fesql::node::kTypeInt32;
        }
        |BIGINT
        {
            $$ = ::fesql::node::kTypeInt64;
        }
        |STRINGTYPE
        {
            $$ = ::fesql::node::kTypeString;
        }
        |FLOAT
        {
            $$ = ::fesql::node::kTypeFloat;
        }
        |DOUBLE
        {
            $$ = ::fesql::node::kTypeDouble;
        }
        |TIMESTAMP
        {
            $$ = ::fesql::node::kTypeTimestamp;
        }
        ;

plist:
     para {
        $$ = node_manager->MakeFnListNode();
        $$->AddChild($1);
     } | para ',' plist  {
        $3->AddChild($1);
        $$ = $3;
     };

para: VARNAME ':' types {
        $$ = node_manager->MakeFnParaNode($1, $3);
    };


primary_time:
    DAYNUM {
        $$ = node_manager->MakeConstNode($1, fesql::node::kTypeDay);
    }
    |HOURNUM {
        $$ = node_manager->MakeConstNode($1, fesql::node::kTypeHour);
    }
    |MINUTENUM {
        $$ = node_manager->MakeConstNode($1, fesql::node::kTypeMinute);
    }
    |SECONDNUM{
        $$ = node_manager->MakeConstNode($1, fesql::node::kTypeSecond);
    }
var: VARNAME {
        $$ = node_manager->MakeFnIdNode($1);
     };

sql_stmt: stmt ';' {
                    trees.push_back($1);
                    YYACCEPT;
                    }
                    ;

   /* statements: select statement */

stmt:   select_stmt
        {
            $$ = $1;
        }
        |create_stmt
        {
            $$ = $1;
        }
        |insert_stmt
        {
        	$$ = $1;
        }
        |cmd_stmt
        {
        	$$ = $1;
        }
        ;



select_stmt:
    SELECT opt_all_clause opt_target_list FROM table_references window_clause limit_clause
            {
                $$ = node_manager->MakeSelectStmtNode($3, $5, $6, $7);
            }
    ;


create_stmt:    CREATE TABLE op_if_not_exist relation_name '(' column_desc_list ')'
                {
                    $$ = node_manager->MakeCreateTableNode($3, $4, $6);
                }
                ;


insert_stmt:	INSERT INTO table_name VALUES '(' insert_expr_list ')'
				{
					$$ = node_manager->MakeInsertTableNode($3, NULL, $6);
				}
				|INSERT INTO table_name '(' column_ref_list ')' VALUES '(' insert_expr_list ')'
				{

					$$ = node_manager->MakeInsertTableNode($3, $5, $9);
				}
				;

column_ref_list:	column_ref
					{
						$$ = node_manager->MakeExprList($1);
					}
					|column_ref_list',' column_ref
					{
						$$ = $1;
						$$->AddChild($3);
					}
					;

insert_expr_list:	insert_expr
					{
						$$ = node_manager->MakeExprList($1);
					}
					| insert_expr_list ',' insert_expr
					{
						$$ = $1;
						$$->PushBack($3);
					}
					;
insert_expr:	expr_const
				;
cmd_stmt:
			CREATE GROUP group_name
			{
				$$ = node_manager->MakeCmdNode(::fesql::node::kCmdCreateGroup, $3);
				free($3);
			}
			|CREATE DATABASE database_name
			{
				$$ = node_manager->MakeCmdNode(::fesql::node::kCmdCreateDatabase, $3);
				free($3);
			}
			|CREATE TABLE file_path
			{

				$$ = node_manager->MakeCmdNode(::fesql::node::kCmdSource, $3);
				free($3);
			}
			|SHOW DATABASES
			{
				$$ = node_manager->MakeCmdNode(::fesql::node::kCmdShowDatabases);
			}
			|SHOW TABLES
			{
				$$ = node_manager->MakeCmdNode(::fesql::node::kCmdShowTables);
			}
			|DESC table_name
			{
				$$ = node_manager->MakeCmdNode(::fesql::node::kCmdDescTable, $2);
				free($2);
			}
			|USE database_name
			{
				$$ = node_manager->MakeCmdNode(::fesql::node::kCmdUseDatabase, $2);
				free($2);
			}
            |EXIT {
                $$ = node_manager->MakeCmdNode(::fesql::node::kCmdExit);
            }
			;

file_path:
			STRING
			{}
			;

column_desc_list:   column_desc
                    {
                        $$ = node_manager->MakeNodeList($1);
                    }
                    | column_desc_list ',' column_desc
                    {
                    	$$ = $1;
                        $$->PushBack($3);
                    }
                    ;

column_desc:    column_name types op_not_null
                {
                    $$ = node_manager->MakeColumnDescNode($1, $2, $3);
                }
                | INDEX '(' column_index_item_list ')'
                {
                    $$ = node_manager->MakeColumnIndexNode($3);
                }
                ;

column_index_item_list:    column_index_item
                    {
                        $$ = node_manager->MakeNodeList($1);
                    }
                    |  column_index_item_list ',' column_index_item
                    {
                        $$ = $1;
                        $$->PushBack($3);
                    }
                    ;

column_index_item:  KEY EQUALS column_name
                    {
                        $$ = node_manager->MakeIndexKeyNode($3);
                    }
                    | KEY EQUALS '(' column_index_key ')'
                    {
                        $$ = $4;
                    }
                    | TS EQUALS column_name
                    {
                        $$ = node_manager->MakeIndexTsNode($3);
                    }
                    | TTL EQUALS primary_time
                    {
                        $$ = node_manager->MakeIndexTTLNode($3);
                    }
                    | VERSION EQUALS column_name
                    {
                        $$ = node_manager->MakeIndexVersionNode($3);
                    }
                    | VERSION EQUALS '(' column_name ',' INTNUM ')'
                    {
                        $$ = node_manager->MakeIndexVersionNode($4, $6);
                    }
                    ;

column_index_key:   column_name
            {
                $$ = node_manager->MakeIndexKeyNode($1);
            }
            | column_index_key ',' column_name
            {
                $$ = $1;
                ((::fesql::node::IndexKeyNode*)$$)->AddKey($3);
            }
            ;


op_if_not_exist:    IF NOT EXISTS
                    {
                        $$ = true;
                    }
                    |/*EMPTY*/
                    {
                        $$ = false;
                    }
                    ;

op_not_null:    NOT NULLX
                {
                    $$ = true;
                }
                |/*EMPTY*/
                {
                    $$ = false;
                }
                ;

opt_all_clause:
        ALL										{ $$ = NULL;}
        | /*EMPTY*/								{ $$ = NULL; }
    ;


/*****************************************************************************
 *
 *	target list for SELECT
 *
 *****************************************************************************/

opt_target_list: select_projection_list						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;
select_projection_list: projection {
                            $$ = node_manager->MakeNodeList($1);
                       }
    | select_projection_list ',' projection
    {
        $$ = $1;
        $$->PushBack($3);
    }
    ;

projection:	expr
			{
				$$ = node_manager->MakeResTargetNode($1, "");
			}
			|expr NAME
			{
        		$$ = node_manager->MakeResTargetNode($1, $2);
				free($2);
    		}
    		| expr AS NAME
    		{
    			$$ = node_manager->MakeResTargetNode($1, $3);
    			free($3);
    		}
    		| '*'
        	{
            	::fesql::node::ExprNode* pNode = node_manager->MakeAllNode("");
            	$$ = node_manager->MakeResTargetNode(pNode, "");
        	}
    		;

over_clause: OVER window_specification
				{ $$ = $2; }
			| OVER NAME
				{
				    $$ = node_manager->MakeWindowDefNode($2);
				    free($2);
				}
			| /*EMPTY*/
				{ $$ = NULL; }
		;

table_references:    table_reference { $$ = node_manager->MakeNodeList($1); }
    |  table_references ',' table_reference
    {
     	$$ = $1;
        $$->PushBack($3);
    }
    ;

table_reference:  table_factor
;


table_factor:
  relation_factor
    {
        $$ = node_manager->MakeTableNode($1, "");
    }
  | relation_factor AS relation_name
    {
        $$ = node_manager->MakeTableNode($1, $3);
    }
  | relation_factor relation_name
    {
        $$ = node_manager->MakeTableNode($1, $2);
    }

  ;
relation_factor:
    relation_name
    { $$ = $1; }


/**** expressions ****/
expr_list:
    expr
    {
      $$ = node_manager->MakeNodeList($1);
    }
  	| expr_list ',' expr
    {
        $$ = $1;
        $$->PushBack($3);
    }
  	;

expr:	column_ref   { $$ = $1; }
     | call_expr  { $$ = $1; }
     | expr_const
     | var
     | expr '+' expr
     {
     	$$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpAdd);
     }
     | expr '-' expr
     {
     	$$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpMinus);
     }
     | expr '*' expr
     {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpMulti);
     }
     | expr '/' expr
     {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpFDiv);
     }
     | expr '%' expr
     {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpMod);
     }
     | expr '>' expr
     {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpGt);
     }
     | expr '<' expr
     {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpLt);
     }
     | expr LESS_EQUALS expr
     {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpLe);
     }
     | expr EQUALS expr
     {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpEq);
     }
     | expr NOT_EQUALS expr
     {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpNeq);
     }
     | expr GREATER_EQUALS expr
     {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpGe);
     }
     | expr ANDOP expr
     {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpAnd);
     }
     | expr OR expr
     {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpOr);
     }
     | '!' expr
     {
        $$ = node_manager->MakeUnaryExprNode($2, ::fesql::node::kFnOpNot);
     }
     | NOT expr
     {
        $$ = node_manager->MakeUnaryExprNode($2, ::fesql::node::kFnOpNot);
     }
     | '(' expr ')'
     {
        $$ = node_manager->MakeUnaryExprNode($2, ::fesql::node::kFnOpBracket);
     }
     ;

expr_const:
    STRING
        {
        	$$ = node_manager->MakeConstNode($1);
			free($1);
        }
  	| INTNUM
        { $$ = (node_manager->MakeConstNode($1)); }
  	| APPROXNUM
        { $$ = (node_manager->MakeConstNode($1)); }
  	| BOOL
        { $$ = (node_manager->MakeConstNode($1)); }
  	| NULLX
        { $$ = (node_manager->MakeConstNode()); }
  	;

call_expr:
    function_name '(' '*' ')' over_clause
    {
          if (strcasecmp($1, "count") != 0)
          {
            yyerror(&(@3), scanner, trees, node_manager, status, "Only COUNT function can be with '*' parameter!");
            YYABORT;
          }
          else
          {
            $$ = node_manager->MakeFuncNode($1, NULL, $5);
          }
    }
    | function_name '(' expr_list ')' over_clause
    {
        $$ = node_manager->MakeFuncNode($1, $3, $5);
    }


/***** Window Definitions */
window_clause:
			WINDOW window_definition_list			{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

window_definition_list:
    window_definition
    {
        $$ = node_manager->MakeNodeList($1);
    }
	| window_definition_list ',' window_definition
	{
	 	$$ = $1;
        $$->PushBack($3);
	}
	;

window_definition:
		NAME AS window_specification
		{
		    ((::fesql::node::WindowDefNode*)$3)->SetName($1);
			free($1);
		    $$ = $3;
		}
		;

window_specification: '(' opt_existing_window_name opt_partition_clause
						opt_sort_clause opt_frame_clause ')'
				{
				    $$ = node_manager->MakeWindowDefNode($3, $4, $5);
				}
		;

opt_existing_window_name:
						NAME { $$ = $1; }
						| /*EMPTY*/		{ $$ = NULL; }

                        ;

opt_partition_clause: PARTITION BY column_ref_list		{ $$ = $3; }
			            | /*EMPTY*/					{ $$ = NULL; }



limit_clause:
            LIMIT INTNUM
            {
                $$ = node_manager->MakeLimitNode($2);
            }
            | /*EMPTY*/ {$$ = NULL;}
            ;
/*===========================================================
 *
 *	Sort By Clasuse
 *
 *===========================================================*/

opt_sort_clause:
			sort_clause								{ $$ = $1;}
			| /*EMPTY*/								{ $$ = NULL; }
		    ;

sort_clause:
			ORDER BY column_ref_list					{ $$ = $3; }
		    ;
/*===========================================================
 *
 *	Frame Clasuse
 *
 *===========================================================*/
opt_frame_clause:
	        RANGE frame_extent opt_window_exclusion_clause
				{
				    $$ = node_manager->MakeRangeFrameNode($2);

				}
			| ROWS frame_extent opt_window_exclusion_clause
				{
				    $$ = node_manager->MakeRowsFrameNode($2);
				}
			|
			/*EMPTY*/
			{
			    $$ = NULL;
		    }
		    ;

opt_window_exclusion_clause:
             /*EMPTY*/				{ $$ = 0; }
            ;
frame_extent: BETWEEN frame_bound AND frame_bound
				{
				    $$ = node_manager->MakeFrameNode($2, $4);
				}
		;


frame_bound:
			UNBOUNDED PRECEDING
				{
				    $$ = (fesql::node::SQLNode*)(node_manager->MakeFrameBound(fesql::node::kPreceding));
				}
			| UNBOUNDED FOLLOWING
				{
				    $$ = (fesql::node::SQLNode*)(node_manager->MakeFrameBound(fesql::node::kFollowing));
				}
			| CURRENT ROW
				{
				    $$ = (fesql::node::SQLNode*)(node_manager->MakeFrameBound(fesql::node::kCurrent));
				}
			| frame_expr PRECEDING
				{
				    $$ = (fesql::node::SQLNode*)(node_manager->MakeFrameBound(fesql::node::kPreceding, $1));
				}
			| frame_expr FOLLOWING
				{
				    $$ = (fesql::node::SQLNode*)(node_manager->MakeFrameBound(fesql::node::kFollowing, $1));
				}
		;

frame_expr: expr_const
			|primary_time
			;
column_ref:
    column_name
    {
        $$ = node_manager->MakeColumnRefNode($1, "");
    }
  | relation_name '.' column_name
    {
        $$ = node_manager->MakeColumnRefNode($3, $1);
    }
  |
    relation_name '.' '*'
    {
        $$ = node_manager->MakeColumnRefNode("*", $1);
    }
  ;

/*===========================================================
 *
 *	Name classification
 *
 *===========================================================*/

database_name:
	NAME
	;

group_name:
	NAME
	;

table_name:
	NAME
	;
column_name:
    NAME
  ;

relation_name:
    NAME
  ;

function_name:
    NAME
  ;

%%


void emit(const char *s, ...)
{
//  	va_list ap;
//  	va_start(ap, s);
//  	printf("rpn: ");
//  	vfprintf(stdout, s, ap);
//  	printf("\n");
}


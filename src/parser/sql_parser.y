%define api.pure full
%locations

%output  "sql_parser.gen.cc"
%defines "sql_parser.gen.h"

// Prefix the parser
%define parse.error verbose
%locations
%lex-param   { yyscan_t scanner }
%parse-param { yyscan_t scanner }
%parse-param { ::fesql::parser::SQLNodeList *nodelist}

%{
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <utility>
#include "parser/node.h"
#include "parser/sql_parser.gen.h"

extern int yylex(YYSTYPE* yylvalp, 
                 YYLTYPE* yyllocp, 
                 yyscan_t scanner);
void emit(char *s, ...);

void yyerror_msg(char *s, ...);
void yyerror(YYLTYPE* yyllocp, yyscan_t unused, ::fesql::parser::SQLNodeList* list , const char* msg ) {
printf("error %s", msg);
}
%}

%code requires {
#include "parser/node.h"
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
	::fesql::parser::SQLNode* node;
	::fesql::parser::SQLNode* target;
	::fesql::parser::SQLNodeList* list;
}

/* names and literal values */
%token <strval> NAME
%token <strval> STRING
%token <intval> INTNUM
%token <intval> BOOL
%token <floatval> APPROXNUM

/* user @abc names */

%token <strval> USERVAR

/* operators and precedence levels */

%right ASSIGN
%left OR
%left XOR
%left ANDOP
%nonassoc IN IS LIKE REGEXP
%left NOT '!'
%left BETWEEN
%left <subtok> COMPARISON /* = <> < > <= >= <=> */
%left '|'
%left '&'
%left <subtok> SHIFT /* << >> */
%left '+' '-'
%left '*' '/' '%' MOD
%left '^'
%nonassoc UMINUS

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
%token ENUM
%token ESCAPED
%token <subtok> EXISTS
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
%token STARTING
%token STRAIGHT_JOIN
%token TABLE
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

%type <node>  sql_stmt stmt select_stmt select_opts select_expr
              opt_all_clause
              table_factor table_reference
              column_ref
              expr simple_expr func_expr expr_const
              sortby opt_frame_clause frame_bound frame_extent
              window_definition window_specification over_clause

%type <target> projection

%type <list> val_list opt_val_list case_list
            opt_target_list
            select_expr_list expr_list
            table_references
            opt_sort_clause sort_clause sortby_list
            window_clause window_definition_list opt_partition_clause


%type <strval> relation_name relation_factor
               column_name
               function_name
               opt_existing_window_name

%type <intval> opt_window_exclusion_clause
%type <node> groupby_list opt_with_rollup opt_asc_desc
%type <node> opt_inner_cross opt_outer
%type <node> left_or_right opt_left_or_right_outer column_list
%type <node> index_list opt_for_join

%type <node> delete_opts delete_list
%type <node> insert_opts insert_vals insert_vals_list
%type <node> insert_asgn_list opt_if_not_exists update_opts update_asgn_list
%type <node> opt_temporary opt_length opt_binary opt_uz enum_list
%type <node> column_atts data_type opt_ignore_replace create_col_list

%start sql_stmt

%%

sql_stmt: stmt ';' {
                            nodelist->PushFront($1);
                            YYACCEPT;}
    ;

   /* statements: select statement */

stmt: select_stmt {
                    $$ = $1;
                    }

   ;

select_stmt:
    SELECT opt_all_clause opt_target_list FROM table_references window_clause
            {
                $$ = ::fesql::parser::MakeSelectStmtNode($3, $5, $6);
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

opt_target_list: select_expr_list						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;
select_expr_list: projection {
                            $$ = ::fesql::parser::MakeNodeList($1);
                       }
    | projection ',' select_expr_list
    {
        ::fesql::parser::SQLNodeList *new_list = ::fesql::parser::MakeNodeList($1);
        new_list->AppendNodeList($3);
        $$ = new_list;
    }
    ;

projection:
    expr
    {
        $$ = ::fesql::parser::MakeResTargetNode($1, "");
    }
    | expr NAME
    {
        $$ = ::fesql::parser::MakeResTargetNode($1, $2);
    }
    | expr AS NAME
    {
        $$ = ::fesql::parser::MakeResTargetNode($1, $3);
    }
    | '*'
        {
            ::fesql::parser::SQLNode *pNode = ::fesql::parser::MakeNode(::fesql::parser::kAll);
            $$ = ::fesql::parser::MakeResTargetNode(pNode, "");
        }
      ;
    ;

over_clause: OVER window_specification
				{ $$ = $2; }
			| OVER NAME
				{
				    $$ = ::fesql::parser::MakeWindowDefNode($2);
				}
			| /*EMPTY*/
				{ $$ = NULL; }
		;

table_references:    table_reference { $$ = ::fesql::parser::MakeNodeList($1); }
    | table_reference ',' table_references
    {
        ::fesql::parser::SQLNodeList *new_list = ::fesql::parser::MakeNodeList($1);
        new_list->AppendNodeList($3);
        $$ = new_list;
    }
    ;

table_reference:  table_factor
;


table_factor:
  relation_factor
    {
        $$ = ::fesql::parser::MakeTableNode($1, "");
    }
  | relation_factor AS relation_name
    {
        $$ = ::fesql::parser::MakeTableNode($1, $3);
    }
  | relation_factor relation_name
    {
        $$ = ::fesql::parser::MakeTableNode($1, $2);
    }

  ;
relation_factor:
    relation_name
    { $$ = $1; }

index_hint:
     USE KEY opt_for_join '(' index_list ')'
                  { emit("INDEXHINT %d %d", $5, 010+$3); }
   | IGNORE KEY opt_for_join '(' index_list ')'
                  { emit("INDEXHINT %d %d", $5, 020+$3); }
   | FORCE KEY opt_for_join '(' index_list ')'
                  { emit("INDEXHINT %d %d", $5, 030+$3); }
   | /* nil */
   ;

index_list: NAME  { emit("INDEX %s", $1); free($1); }
   | index_list ',' NAME { emit("INDEX %s", $3); free($3); $$ = $1 + 1; }
   ;

opt_for_join: FOR JOIN { }
   | /* nil */ { $$ = 0; }
   ;

opt_as: AS
  | /* nil */
  ;

opt_as_alias: AS NAME {
                        emit("enter opt as alias");
                        emit ("ALIAS %s", $2); free($2); }
  | NAME              {  emit("enter opt as alias");
                        emit ("ALIAS %s", $1); free($1); }
  | /* nil */           { emit("enter opt as alias");}
  ;


/**** expressions ****/
expr_list:
    expr
    {
      $$ = ::fesql::parser::MakeNodeList($1);
    }
  | expr ',' expr_list
    {
        ::fesql::parser::SQLNodeList *new_list = ::fesql::parser::MakeNodeList($1);
        new_list->AppendNodeList($3);
        $$ = new_list;
    }
  ;
expr:
    simple_expr   { $$ = $1; }
    |func_expr  { $$ = $1; }
    ;

simple_expr:
    column_ref
        { $$ = $1; }
    | expr_const
        { $$ = $1; }

    ;

expr_const:
    STRING
        { $$ = (::fesql::parser::SQLNode*)(new ::fesql::parser::ConstNode($1)); }
  | INTNUM
        { $$ = (::fesql::parser::SQLNode*)(new ::fesql::parser::ConstNode($1)); }
  | APPROXNUM
        { $$ = (::fesql::parser::SQLNode*)(new ::fesql::parser::ConstNode($1)); }
  | BOOL
        { $$ = (::fesql::parser::SQLNode*)(new ::fesql::parser::ConstNode($1)); }
  | NULLX
        { $$ = (::fesql::parser::SQLNode*)(new ::fesql::parser::ConstNode()); }
  ;
func_expr:
    function_name '(' '*' ')' over_clause
    {
          if (strcasecmp($1, "count") != 0)
          {
            yyerror_msg("Only COUNT function can be with '*' parameter!");
            YYABORT;
          }
          else
          {
            $$ = ::fesql::parser::MakeFuncNode($1, NULL, $5);
          }
    }
    | function_name '(' expr_list ')' over_clause
    {
        $$ = ::fesql::parser::MakeFuncNode($1, $3, $5);
    }

/***** Window Definitions */
window_clause:
			WINDOW window_definition_list			{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

window_definition_list:
    window_definition
    {
        $$ = ::fesql::parser::MakeNodeList($1);
    }
	| window_definition ',' window_definition_list
	{
        ::fesql::parser::SQLNodeList *new_list = ::fesql::parser::MakeNodeList($1);
        new_list->AppendNodeList($3);
        $$ = new_list;
	}
	;

window_definition:
		NAME AS window_specification
		{
		    ((::fesql::parser::WindowDefNode*)$3)->SetName($1);
		    $$ = $3;
		}
		;

window_specification: '(' opt_existing_window_name opt_partition_clause
						opt_sort_clause opt_frame_clause ')'
				{
				    $$ = ::fesql::parser::MakeWindowDefNode($3, $4, $5);
				}
		;

opt_existing_window_name:
						NAME { $$ = $1; }
                        | /*EMPTY*/		%prec Op    { $$ = NULL; }
                        ;
opt_partition_clause: PARTITION BY expr_list		{ $$ = $3; }
			            | /*EMPTY*/					{ $$ = NULL; }


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
			ORDER BY sortby_list					{ $$ = $3; }
		    ;

sortby_list:
			sortby
			{
			     $$ = ::fesql::parser::MakeNodeList($1);
			}
			|sortby ',' sortby_list
			{
			    ::fesql::parser::SQLNodeList *new_list = ::fesql::parser::MakeNodeList($1);
                new_list->AppendNodeList($3);
                $$ = new_list;
			}
		    ;

sortby:	column_name
		{
		    ::fesql::parser::SQLNode* node_ptr = ::fesql::parser::MakeColumnRefNode($1, "");
		    $$ = ::fesql::parser::MakeOrderByNode(node_ptr);
		}
		;

/*===========================================================
 *
 *	Frame Clasuse
 *
 *===========================================================*/
opt_frame_clause:
	        RANGE frame_extent opt_window_exclusion_clause
				{
				    $$ = ::fesql::parser::MakeRangeFrameNode($2);

				}
			| ROWS frame_extent opt_window_exclusion_clause
				{
				    $$ = ::fesql::parser::MakeRowsFrameNode($2);
				}
			|
			/*EMPTY*/
			{
			    $$ = NULL;
		    }
		    ;

opt_window_exclusion_clause:
            | /*EMPTY*/				{ $$ = 0; }
            ;
frame_extent: frame_bound
				{
				    $$ = ::fesql::parser::MakeFrameNode($1, NULL);
				}
			| BETWEEN frame_bound AND frame_bound
				{
				    $$ = ::fesql::parser::MakeFrameNode($2, $4);
				}
		;


frame_bound:
			UNBOUNDED PRECEDING
				{
				    $$ = (fesql::parser::SQLNode*)(new ::fesql::parser::FrameBound(fesql::parser::kPreceding));
				}
			| UNBOUNDED FOLLOWING
				{
				    $$ = (fesql::parser::SQLNode*)(new ::fesql::parser::FrameBound(fesql::parser::kFollowing));
				}
			| CURRENT ROW
				{
				    $$ = (fesql::parser::SQLNode*)(new ::fesql::parser::FrameBound(fesql::parser::kCurrent));
				}
			| expr PRECEDING
				{
				    $$ = (fesql::parser::SQLNode*)(new ::fesql::parser::FrameBound(fesql::parser::kPreceding, $1));
				}
			| expr FOLLOWING
				{
				    $$ = (fesql::parser::SQLNode*)(new ::fesql::parser::FrameBound(fesql::parser::kFollowing, $1));
				}
		;

column_ref:
    column_name
    {
        $$ = ::fesql::parser::MakeColumnRefNode($1, "");
    }
  | relation_name '.' column_name
    {
        $$ = ::fesql::parser::MakeColumnRefNode($3, $1);
    }
  |
    relation_name '.' '*'
    {
        $$ = ::fesql::parser::MakeColumnRefNode("*", $1);
    }
  ;

/*===========================================================
 *
 *	Name classification
 *
 *===========================================================*/


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


void emit(char *s, ...)
{

  va_list ap;
  va_start(ap, s);
  printf("rpn: ");
  vfprintf(stdout, s, ap);
  printf("\n");
}

void yyerror_msg(char *s, ...)
{

  va_list ap;
  va_start(ap, s);

  vfprintf(stderr, s, ap);
  fprintf(stderr, "\n");
}


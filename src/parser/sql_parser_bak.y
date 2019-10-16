%define api.pure full
%locations

%output  "sql_parser.gen.cc"
%defines "sql_parser.gen.h"

// Prefix the parser
%define parse.error verbose
%locations
%lex-param   { yyscan_t scanner }
%parse-param { yyscan_t scanner }
%parse-param { ::fedb::sql::SQLNode* root }

%{
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include "parser/node.h"
#include "parser/sql_parser.gen.h"

extern int yylex(YYSTYPE* yylvalp, 
                 YYLTYPE* yyllocp, 
                 yyscan_t scanner);
void emit(char *s, ...);

void yyerror_msg(char *s, ...);
void yyerror(YYLTYPE* yyllocp, yyscan_t unused, ::fedb::sql::SQLNode* node , const char* msg ) {
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
	char *strval;
	int subtok;
	::fedb::sql::SQLNode* node;
	::fedb::sql::SQLNode* target;
	int list;
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
%token PRECISION
%token PRIMARY
%token PROCEDURE
%token PURGE
%token QUICK
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
%token RIGHT
%token ROLLUP
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
%token UTC_DATE
%token UTC_TIME
%token UTC_TIMESTAMP
%token VALUES
%token VARBINARY
%token VARCHAR
%token VARYING
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

%type <node>  select_opts select_expr opt_all_clause expr
%type <target> target_el
%type <list> val_list opt_val_list case_list
            opt_target_list target_list
%type <node> groupby_list opt_with_rollup opt_asc_desc
%type <node> table_references opt_inner_cross opt_outer
%type <node> left_or_right opt_left_or_right_outer column_list
%type <node> index_list opt_for_join

%type <node> delete_opts delete_list
%type <node> insert_opts insert_vals insert_vals_list
%type <node> insert_asgn_list opt_if_not_exists update_opts update_asgn_list
%type <node> opt_temporary opt_length opt_binary opt_uz enum_list
%type <node> column_atts data_type opt_ignore_replace create_col_list

%start stmt_list

%%

stmt_list: stmt ';'
  | stmt_list stmt ';'
  ;

   /* statements: select statement */

stmt: select_stmt { emit("STMT"); }
   ;

select_stmt:
        SELECT opt_all_clause opt_target_list FROM table_references
     opt_where opt_groupby opt_having opt_orderby opt_limit
     opt_into_list { emit("SELECT %s %s %s", $2, $3, $5); } ;
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

opt_target_list: target_list						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;
target_list: target_el { $$ = 1; }
    | target_list ',' target_el {$$ = $1 + 1; }
    | '*' { emit("SELECTALL"); $$ = 1; }
    ;


target_el: expr AS NAME
				{
				    emit("enter target el");
					$$ = ::fedb::sql::MakeNode(::fedb::sql::kResTarget);
					((fedb::sql::ResTarget*)$$)->setName($3);
					((fedb::sql::ResTarget*)$$)->setVal($1);
				}
	;
opt_where: /* nil */ 
   | WHERE expr { emit("WHERE"); };

opt_groupby: /* nil */ 
   | GROUP BY groupby_list opt_with_rollup
                             { emit("GROUPBYLIST %d %d", $3, $4); }
   ;

groupby_list: expr opt_asc_desc
                             { emit("GROUPBY %d",  $2);}
   | groupby_list ',' expr opt_asc_desc
                             { emit("GROUPBY %d",  $4); }
   ;

opt_asc_desc: /* nil */ { }
   | ASC                { emit("ASC");}
   | DESC               {  emit("DESC"); }
    ;

opt_with_rollup: /* nil */  { $$ = 0; }
   | WITH ROLLUP  { $$ = NULL; }
   ;

opt_having: /* nil */ | HAVING expr { emit("HAVING"); };

opt_orderby: /* nil */ | ORDER BY groupby_list { emit("ORDERBY %d", $3); }
   ;

opt_limit: /* nil */ | LIMIT expr { emit("LIMIT 1"); }
  | LIMIT expr ',' expr             { emit("LIMIT 2"); }
  ; 

opt_into_list: /* nil */ 
   | INTO column_list { emit("INTO %d", $2); }
   ;

column_list: NAME { emit("COLUMN %s", $1); free($1);  }
  | column_list ',' NAME  { emit("COLUMN %s", $3); free($3); }
  ;

select_opts:                          { $$ = NULL; }
| select_opts ALL                 {  yyerror_msg("duplicate ALL option");  }
| select_opts DISTINCT            {  yyerror_msg("duplicate DISTINCT option");  }
| select_opts DISTINCTROW         {  yyerror_msg("duplicate DISTINCTROW option"); }
| select_opts HIGH_PRIORITY       {  yyerror_msg("duplicate HIGH_PRIORITY option");  }
| select_opts STRAIGHT_JOIN       {  yyerror_msg("duplicate STRAIGHT_JOIN option");  }
| select_opts SQL_SMALL_RESULT    { yyerror_msg("duplicate SQL_SMALL_RESULT option");  }
| select_opts SQL_BIG_RESULT      { yyerror_msg("duplicate SQL_BIG_RESULT option");  }
| select_opts SQL_CALC_FOUND_ROWS { yyerror_msg("duplicate SQL_CALC_FOUND_ROWS option"); }
    ;


select_expr: expr opt_as_alias ;

table_references:    table_reference { $$ = NULL; }
    | table_references ',' table_reference { $$ = NULL; }
    ;

table_reference:  table_factor
  | join_table
;

table_factor:
    NAME opt_as_alias index_hint { emit("TABLE %s", $1); free($1); }
  | NAME '.' NAME opt_as_alias index_hint { emit("TABLE %s.%s", $1, $3);
                               free($1); free($3); }
  | table_subquery opt_as NAME { emit("SUBQUERYAS %s", $3); free($3); }
  | '(' table_references ')' { emit("TABLEREFERENCES %d", $2); }
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

join_table:
    table_reference opt_inner_cross JOIN table_factor opt_join_condition
                  { emit("JOIN"); }
  | table_reference STRAIGHT_JOIN table_factor
                  { emit("JOIN"); }
  | table_reference STRAIGHT_JOIN table_factor ON expr
                  { emit("JOIN"); }
  | table_reference left_or_right opt_outer JOIN table_factor join_condition
                  { emit("JOIN"); }
  | table_reference NATURAL opt_left_or_right_outer JOIN table_factor
                  { emit("JOIN"); }
  ;

opt_inner_cross: /* nil */ {}
   | INNER {  }
   | CROSS  { }
;

opt_outer: /* nil */  {  }
   | OUTER { }
   ;

left_or_right: LEFT {  }
    | RIGHT {  }
    ;

opt_left_or_right_outer: LEFT opt_outer {  }
   | RIGHT opt_outer  { }
   | /* nil */ {  }
   ;

opt_join_condition: join_condition | /* nil */ ;

join_condition:
    ON expr { emit("ONEXPR"); }
    | USING '(' column_list ')' { emit("USING %d", $3); }
    ;

index_hint:
     USE KEY opt_for_join '(' index_list ')'
                  { emit("INDEXHINT %d %d", $5, 010+$3); }
   | IGNORE KEY opt_for_join '(' index_list ')'
                  { emit("INDEXHINT %d %d", $5, 020+$3); }
   | FORCE KEY opt_for_join '(' index_list ')'
                  { emit("INDEXHINT %d %d", $5, 030+$3); }
   | /* nil */
   ;

opt_for_join: FOR JOIN { }
   | /* nil */ { $$ = 0; }
   ;

index_list: NAME  { emit("INDEX %s", $1); free($1); }
   | index_list ',' NAME { emit("INDEX %s", $3); free($3); $$ = $1 + 1; }
   ;

table_subquery: '(' select_stmt ')' { emit("SUBQUERY"); }
   ;

   /* statements: delete statement */

stmt: delete_stmt { emit("STMT"); }
   ;

delete_stmt: DELETE delete_opts FROM NAME
    opt_where opt_orderby opt_limit
                  { emit("DELETEONE %d %s", $2, $4); free($4); }
;

delete_opts: delete_opts LOW_PRIORITY { $$ = $1 + 01; }
   | delete_opts QUICK { $$ = $1 + 02; }
   | delete_opts IGNORE { $$ = $1 + 04; }
   | /* nil */ { $$ = 0; }
   ;

delete_stmt: DELETE delete_opts
    delete_list
    FROM table_references opt_where
            { emit("DELETEMULTI %d %d %d", $2, $3, $5); }

delete_list: NAME opt_dot_star { emit("TABLE %s", $1); free($1);  }
   | delete_list ',' NAME opt_dot_star
            { emit("TABLE %s", $3); free($3); $$ = $1 + 1; }
   ;

opt_dot_star: /* nil */ | '.' '*' ;

delete_stmt: DELETE delete_opts
    FROM delete_list
    USING table_references opt_where
            { emit("DELETEMULTI %d %d %d", $2, $4, $6); }
;

   /* statements: insert statement */

stmt: insert_stmt { emit("STMT"); }
   ;

insert_stmt: INSERT insert_opts opt_into NAME
     opt_col_names
     VALUES insert_vals_list
     opt_ondupupdate { emit("INSERTVALS %d %d %s", $2, $7, $4); free($4); }
   ;

opt_ondupupdate: /* nil */
   | ONDUPLICATE KEY UPDATE insert_asgn_list { emit("DUPUPDATE %d", $4); }
   ;

insert_opts: /* nil */ { $$ = 0; }
   | insert_opts LOW_PRIORITY {  }
   | insert_opts DELAYED {  }
   | insert_opts HIGH_PRIORITY {  }
   | insert_opts IGNORE { }
   ;

opt_into: INTO | /* nil */
   ;

opt_col_names: /* nil */
   | '(' column_list ')' { emit("INSERTCOLS %d", $2); }
   ;

insert_vals_list: '(' insert_vals ')' { emit("VALUES %d", $2); }
   | insert_vals_list ',' '(' insert_vals ')' { emit("VALUES %d", $4); $$ = $1 + 1; }

insert_vals:
     expr {}
   | DEFAULT { emit("DEFAULT");  }
   | insert_vals ',' expr {  }
   | insert_vals ',' DEFAULT { emit("DEFAULT");  }
   ;

insert_stmt: INSERT insert_opts opt_into NAME
    SET insert_asgn_list
    opt_ondupupdate
     { emit("INSERTASGN %d %d %s", $2, $6, $4); free($4); }
   ;

insert_stmt: INSERT insert_opts opt_into NAME opt_col_names
    select_stmt
    opt_ondupupdate { emit("INSERTSELECT %d %s", $2, $4); free($4); }
  ;

insert_asgn_list:
     NAME COMPARISON expr 
     { if ($2 != 4) yyerror_msg("bad insert assignment to %s", $1);
       emit("ASSIGN %s", $1); free($1); }
   | NAME COMPARISON DEFAULT
               { if ($2 != 4) yyerror_msg("bad insert assignment to %s", $1);
                 emit("DEFAULT"); emit("ASSIGN %s", $1); free($1); }
   | insert_asgn_list ',' NAME COMPARISON expr
               { if ($4 != 4) yyerror_msg("bad insert assignment to %s", $1);
                 emit("ASSIGN %s", $3); free($3); $$ = $1 + 1; }
   | insert_asgn_list ',' NAME COMPARISON DEFAULT
               { if ($4 != 4) yyerror_msg("bad insert assignment to %s", $1);
                 emit("DEFAULT"); emit("ASSIGN %s", $3); free($3);  }
   ;

   /** replace just like insert **/
stmt: replace_stmt { emit("STMT"); }
   ;

replace_stmt: REPLACE insert_opts opt_into NAME
     opt_col_names
     VALUES insert_vals_list
     opt_ondupupdate { emit("REPLACEVALS %d %d %s", $2, $7, $4); free($4); }
   ;

replace_stmt: REPLACE insert_opts opt_into NAME
    SET insert_asgn_list
    opt_ondupupdate
     { emit("REPLACEASGN %d %d %s", $2, $6, $4); free($4); }
   ;

replace_stmt: REPLACE insert_opts opt_into NAME opt_col_names
    select_stmt
    opt_ondupupdate { emit("REPLACESELECT %d %s", $2, $4); free($4); }
  ;

/** update **/
stmt: update_stmt { emit("STMT"); }
   ;

update_stmt: UPDATE update_opts table_references
    SET update_asgn_list
    opt_where
    opt_orderby
opt_limit { emit("UPDATE %d %d %d", $2, $3, $5); }
;

update_opts: /* nil */ { $$ = 0; }
   | insert_opts LOW_PRIORITY { }
   | insert_opts IGNORE {  }
   ;

update_asgn_list:
     NAME COMPARISON expr 
       { if ($2 != 4) yyerror_msg("bad insert assignment to %s", $1);
	 emit("ASSIGN %s", $1); free($1); }
   | NAME '.' NAME COMPARISON expr 
       { if ($4 != 4) yyerror_msg("bad insert assignment to %s", $1);
	 emit("ASSIGN %s.%s", $1, $3); free($1); free($3); }
   | update_asgn_list ',' NAME COMPARISON expr
       { if ($4 != 4) yyerror_msg("bad insert assignment to %s", $3);
	 emit("ASSIGN %s.%s", $3); free($3); $$ = $1 + 1; }
   | update_asgn_list ',' NAME '.' NAME COMPARISON expr
       { if ($6 != 4) yyerror_msg("bad insert assignment to %s.$s", $3, $5);
	 emit("ASSIGN %s.%s", $3, $5); free($3); free($5); }
   ;


   /** create database **/

stmt: create_database_stmt { emit("STMT"); }
   ;

create_database_stmt: 
     CREATE DATABASE opt_if_not_exists NAME { emit("CREATEDATABASE %d %s", $3, $4); free($4); }
   | CREATE SCHEMA opt_if_not_exists NAME { emit("CREATEDATABASE %d %s", $3, $4); free($4); }
   ;

opt_if_not_exists:  /* nil */ { $$ = 0; }
   | IF EXISTS           { if(!$2)yyerror_msg("IF EXISTS doesn't exist");
                         /* NOT EXISTS hack */ }
   ;


   /** create table **/
stmt: create_table_stmt { emit("STMT"); }
   ;

create_table_stmt: CREATE opt_temporary TABLE opt_if_not_exists NAME
   '(' create_col_list ')' { emit("CREATE %d %d %d %s", $2, $4, $7, $5); free($5); }
   ;

create_table_stmt: CREATE opt_temporary TABLE opt_if_not_exists NAME '.' NAME
   '(' create_col_list ')' { emit("CREATE %d %d %d %s.%s", $2, $4, $9, $5, $7);
                          free($5); free($7); }
   ;

create_table_stmt: CREATE opt_temporary TABLE opt_if_not_exists NAME
   '(' create_col_list ')'
create_select_statement { emit("CREATESELECT %d %d %d %s", $2, $4, $7, $5); free($5); }
    ;

create_table_stmt: CREATE opt_temporary TABLE opt_if_not_exists NAME
   create_select_statement { emit("CREATESELECT %d %d 0 %s", $2, $4, $5); free($5); }
    ;

create_table_stmt: CREATE opt_temporary TABLE opt_if_not_exists NAME '.' NAME
   '(' create_col_list ')'
   create_select_statement  { emit("CREATESELECT %d %d 0 %s.%s", $2, $4, $5, $7);
                              free($5); free($7); }
    ;

create_table_stmt: CREATE opt_temporary TABLE opt_if_not_exists NAME '.' NAME
   create_select_statement { emit("CREATESELECT %d %d 0 %s.%s", $2, $4, $5, $7);
                          free($5); free($7); }
    ;

create_col_list: create_definition {  }
    | create_col_list ',' create_definition { $$ = $1 + 1; }
    ;

create_definition: { emit("STARTCOL"); } NAME data_type column_atts
                   { emit("COLUMNDEF %d %s", $3, $2); free($2); }

    | PRIMARY KEY '(' column_list ')'    { emit("PRIKEY %d", $4); }
    | KEY '(' column_list ')'            { emit("KEY %d", $3); }
    | INDEX '(' column_list ')'          { emit("KEY %d", $3); }
    | FULLTEXT INDEX '(' column_list ')' { emit("TEXTINDEX %d", $4); }
    | FULLTEXT KEY '(' column_list ')'   { emit("TEXTINDEX %d", $4); }
    ;

column_atts: /* nil */ { $$ = 0; }
    | column_atts NOT NULLX             { emit("ATTR NOTNULL"); $$ = $1 + 1; }
    | column_atts NULLX
    | column_atts DEFAULT STRING        { emit("ATTR DEFAULT STRING %s", $3); free($3); $$ = $1 + 1; }
    | column_atts DEFAULT INTNUM        { emit("ATTR DEFAULT NUMBER %d", $3); $$ = $1 + 1; }
    | column_atts DEFAULT APPROXNUM     { emit("ATTR DEFAULT FLOAT %g", $3); $$ = $1 + 1; }
    | column_atts DEFAULT BOOL          { emit("ATTR DEFAULT BOOL %d", $3); $$ = $1 + 1; }
    | column_atts AUTO_INCREMENT        { emit("ATTR AUTOINC"); $$ = $1 + 1; }
    | column_atts UNIQUE '(' column_list ')' { emit("ATTR UNIQUEKEY %d", $4); $$ = $1 + 1; }
    | column_atts UNIQUE KEY { emit("ATTR UNIQUEKEY"); $$ = $1 + 1; }
    | column_atts PRIMARY KEY { emit("ATTR PRIKEY"); $$ = $1 + 1; }
    | column_atts KEY { emit("ATTR PRIKEY"); $$ = $1 + 1; }
    | column_atts COMMENT STRING { emit("ATTR COMMENT %s", $3); free($3); $$ = $1 + 1; }
    ;

opt_length: /* nil */ { $$ = 0; }
   | '(' INTNUM ')' {  }
   | '(' INTNUM ',' INTNUM ')' { }
   ;

opt_binary: /* nil */ { $$ = 0; }
   | BINARY {  }
   ;

opt_uz: /* nil */ { $$ = 0; }
   | opt_uz UNSIGNED {  }
   | opt_uz ZEROFILL {  }
   ;

opt_csc: /* nil */
   | opt_csc CHAR SET STRING { emit("COLCHARSET %s", $4); free($4); }
   | opt_csc COLLATE STRING { emit("COLCOLLATE %s", $3); free($3); }
   ;

data_type:
     BIT opt_length {}
   | TINYINT opt_length opt_uz { }
   | SMALLINT opt_length opt_uz {  }
   | MEDIUMINT opt_length opt_uz { }
   | INT opt_length opt_uz { }
   | INTEGER opt_length opt_uz { }
   | BIGINT opt_length opt_uz {  }
   | REAL opt_length opt_uz { }
   | DOUBLE opt_length opt_uz {  }
   | FLOAT opt_length opt_uz { }
   | DECIMAL opt_length opt_uz {}
   | DATE {  }
   | TIME {  }
   | TIMESTAMP {  }
   | DATETIME { }
   | YEAR {}
   | CHAR opt_length opt_csc {  }
   | VARCHAR '(' INTNUM ')' opt_csc {  }
   | BINARY opt_length {}
   | VARBINARY '(' INTNUM ')' { }
   | TINYBLOB {  }
   | BLOB {  }
   | MEDIUMBLOB {  }
   | LONGBLOB {}
   | TINYTEXT opt_binary opt_csc {}
   | TEXT opt_binary opt_csc {}
   | MEDIUMTEXT opt_binary opt_csc {}
   | LONGTEXT opt_binary opt_csc {}
   | ENUM '(' enum_list ')' opt_csc {}
   | SET '(' enum_list ')' opt_csc {}
   ;

enum_list: STRING { emit("ENUMVAL %s", $1); free($1);}
   | enum_list ',' STRING { emit("ENUMVAL %s", $3); free($3);}
   ;

create_select_statement: opt_ignore_replace opt_as select_stmt {
					   emit("CREATESELECT %d", $1); }
   ;

opt_ignore_replace: /* nil */ { $$ = 0; }
   | IGNORE {  }
   | REPLACE {  }
   ;

opt_temporary:   /* nil */ { $$ = 0; }
   | TEMPORARY { }
   ;

   /**** set user variables ****/

stmt: set_stmt { emit("STMT"); }
   ;

set_stmt: SET set_list ;

set_list: set_expr | set_list ',' set_expr ;

set_expr:
      USERVAR COMPARISON expr { if ($2 != 4) yyerror_msg("bad set to @%s", $1);
		 emit("SET %s", $1); free($1); }
    | USERVAR ASSIGN expr { emit("SET %s", $1); free($1); }
    ;

/**** expressions ****/
expr: NAME          {
                        emit("expr name >>");
                        $$ = ::fedb::sql::MakeNode(::fedb::sql::kConst);
                        ((::fedb::sql::ConstNode*)$$)->setValue($1);
                        emit("NAME %s", $1);
                    }
   | USERVAR         { emit("USERVAR %s", $1); free($1); }
   | NAME '.' NAME { emit("FIELDNAME %s.%s", $1, $3); free($1); free($3); }
   | STRING        { emit("STRING %s", $1); free($1); }
   | INTNUM        { emit("NUMBER %d", $1); }
   | APPROXNUM     { emit("FLOAT %g", $1); }
   | BOOL          { emit("BOOL %d", $1); }
   ;

expr: expr '+' expr { emit("ADD"); }
   | expr '-' expr { emit("SUB"); }
   | expr '*' expr { emit("MUL"); }
   | expr '/' expr { emit("DIV"); }
   | expr '%' expr { emit("MOD"); }
   | expr MOD expr { emit("MOD"); }
   | '-' expr %prec UMINUS { emit("NEG"); }
   | expr ANDOP expr { emit("AND"); }
   | expr OR expr { emit("OR"); }
   | expr XOR expr { emit("XOR"); }
   | expr COMPARISON expr { emit("CMP %d", $2); }
   | expr COMPARISON '(' select_stmt ')' { emit("CMPSELECT %d", $2); }
   | expr COMPARISON ANY '(' select_stmt ')' { emit("CMPANYSELECT %d", $2); }
   | expr COMPARISON SOME '(' select_stmt ')' { emit("CMPANYSELECT %d", $2); }
   | expr COMPARISON ALL '(' select_stmt ')' { emit("CMPALLSELECT %d", $2); }
   | expr '|' expr { emit("BITOR"); }
   | expr '&' expr { emit("BITAND"); }
   | expr '^' expr { emit("BITXOR"); }
   | expr SHIFT expr { emit("SHIFT %s", $2==1?"left":"right"); }
   | NOT expr { emit("NOT"); }
   | '!' expr { emit("NOT"); }
   | USERVAR ASSIGN expr { emit("ASSIGN @%s", $1); free($1); }
   ;    

expr:  expr IS NULLX     { emit("ISNULL"); }
   |   expr IS NOT NULLX { emit("ISNULL"); emit("NOT"); }
   |   expr IS BOOL      { emit("ISBOOL %d", $3); }
   |   expr IS NOT BOOL  { emit("ISBOOL %d", $4); emit("NOT"); }
   ;

expr: expr BETWEEN expr AND expr %prec BETWEEN { emit("BETWEEN"); }
   ;


val_list: expr { $$ = 1; }
   | expr ',' val_list { $$ = 1 + $3; }
   ;

opt_val_list: /* nil */ { $$ = 0; }
   | val_list
   ;

expr: expr IN '(' val_list ')'       { emit("ISIN %d", $4); }
   | expr NOT IN '(' val_list ')'    { emit("ISIN %d", $5); emit("NOT"); }
   | expr IN '(' select_stmt ')'     { emit("INSELECT"); }
   | expr NOT IN '(' select_stmt ')' { emit("INSELECT"); emit("NOT"); }
   | EXISTS '(' select_stmt ')'      { emit("EXISTS"); if($1)emit("NOT"); }
   ;

expr: NAME '(' opt_val_list ')' {  emit("CALL %d %s", $3, $1); free($1); }
   ;

  /* functions with special syntax */
expr: FCOUNT '(' '*' ')' { emit("COUNTALL"); }
   | FCOUNT '(' expr ')' { emit(" CALL 1 COUNT"); } 

expr: FSUBSTRING '(' val_list ')' {  emit("CALL %d SUBSTR", $3);}
   | FSUBSTRING '(' expr FROM expr ')' {  emit("CALL 2 SUBSTR"); }
   | FSUBSTRING '(' expr FROM expr FOR expr ')' {  emit("CALL 3 SUBSTR"); }
| FTRIM '(' val_list ')' { emit("CALL %d TRIM", $3); }
   | FTRIM '(' trim_ltb expr FROM val_list ')' { emit("CALL 3 TRIM"); }
   ;

trim_ltb: LEADING { emit("INT 1"); }
   | TRAILING { emit("INT 2"); }
   | BOTH { emit("INT 3"); }
   ;

expr: FDATE_ADD '(' expr ',' interval_exp ')' { emit("CALL 3 DATE_ADD"); }
   |  FDATE_SUB '(' expr ',' interval_exp ')' { emit("CALL 3 DATE_SUB"); }
   ;

interval_exp: INTERVAL expr DAY_HOUR { emit("NUMBER 1"); }
   | INTERVAL expr DAY_MICROSECOND { emit("NUMBER 2"); }
   | INTERVAL expr DAY_MINUTE { emit("NUMBER 3"); }
   | INTERVAL expr DAY_SECOND { emit("NUMBER 4"); }
   | INTERVAL expr YEAR_MONTH { emit("NUMBER 5"); }
   | INTERVAL expr YEAR       { emit("NUMBER 6"); }
   | INTERVAL expr HOUR_MICROSECOND { emit("NUMBER 7"); }
   | INTERVAL expr HOUR_MINUTE { emit("NUMBER 8"); }
   | INTERVAL expr HOUR_SECOND { emit("NUMBER 9"); }
   ;

expr: CASE expr case_list END           { emit("CASEVAL %d 0", $3); }
   |  CASE expr case_list ELSE expr END { emit("CASEVAL %d 1", $3); }
   |  CASE case_list END                { emit("CASE %d 0", $2); }
   |  CASE case_list ELSE expr END      { emit("CASE %d 1", $2); }
   ;

case_list: WHEN expr THEN expr     { $$ = 1; }
         | case_list WHEN expr THEN expr { $$ = $1+1; } 
   ;

expr: expr LIKE expr { emit("LIKE"); }
   | expr NOT LIKE expr { emit("LIKE"); emit("NOT"); }
   ;

expr: expr REGEXP expr { emit("REGEXP"); }
   | expr NOT REGEXP expr { emit("REGEXP"); emit("NOT"); }
   ;

expr: CURRENT_TIMESTAMP { emit("NOW"); }
   | CURRENT_DATE	{ emit("NOW"); }
   | CURRENT_TIME	{ emit("NOW"); }
   ;

expr: BINARY expr %prec UMINUS { emit("STRTOBIN"); }
   ;

%%

void
emit(char *s, ...)
{

  va_list ap;
  va_start(ap, s);
  printf("rpn: ");
  vfprintf(stdout, s, ap);
  printf("\n");
}

void
yyerror_msg(char *s, ...)
{

  va_list ap;
  va_start(ap, s);

  vfprintf(stderr, s, ap);
  fprintf(stderr, "\n");
}


%option noinput nounput noyywrap 8bit nodefault
%option reentrant bison-bridge bison-locations case-insensitive
%option yylineno
%option outfile="sql_scan.gen.cc"

%{
#include "node/node_manager.h"
#include "parser/sql_parser.gen.h"
#include <stdarg.h>
#include <string.h>

void yyerror(const char *s, ...) {}
int cur_state=0;
int oldstate = 0;
int columnpos = 0;
#define YY_USER_ACTION {yylloc->first_line = yylineno; \
        yylloc->first_column = yycolumn;                 \
        yylloc->last_column = yycolumn;                    \
        yycolumn=yycolumn+yyleng;                         \
        yylloc->last_line = yylineno;}

inline unsigned char escaped_char(unsigned char c)
{
  switch (c)
  {
    case 'b':
      return '\b';
    case 'f':
      return '\f';
    case 'n':
      return '\n';
    case 'r':
      return '\r';
    case 't':
      return '\t';
    default:
      return c;
  }
}

int64_t parse_string(const char *src, char *dest, int64_t len, int quote_type) {
    int64_t i;
    int64_t index = 0;
    for (i = 0; i < len; ++i) {
        unsigned char c = src[i];
        if (c == '\\') {
            if (i < len - 1) {
                c = src[++i];
            } else {
                break;
            }
            c = escaped_char(c);
        } else if (quote_type == 0 && c == '\'' && i + 1 < len &&
                   src[i + 1] == '\'') {
            ++i;
        } else if (quote_type == 1 && c == '"' && i + 1 < len &&
                   src[i + 1] == '"') {
            ++i;
        }
        dest[index++] = c;
    }
    dest[index] = '\0';
    return index;
}
%}

space [ \t]
newlines [\n\r]+
non_newline [^\n\r]
comment ("#"{non_newline}*)
indent {space}*
less_equals		"<="
greater_equals	">="
not_equals		"!="
equals  "=="
add_assign "+="
minus_assign "-="
multi_assign "*="
fdiv_assign "/="
assign  "="
and "&&"
or  "||"
self [,()\[\].\:\+\-\*\/\%\^\<\>]

%x COMMENT
%s FUN
%s FUN_INDENT
%s BTWMODE

ident_start [A-Za-z\200-\377_]
ident_cont [A-Za-z\200-\377_0-9\$]
identifier {ident_start}{ident_cont}*

%%

"%%sql\n" {columnpos = 1; BEGIN INITIAL;}
"%%fun\n" {columnpos = 1;BEGIN FUN_INDENT;}

 /* keywords */

ADD	{ return ADD; }
ALL	{ return ALL; }
ALTER	{ return ALTER; }
ANALYZE	{ return ANALYZE; }

    /* Hack for BETWEEN ... AND ...
     * return special AND token if BETWEEN seen
     */
<BTWMODE>AND	{ BEGIN INITIAL; return AND; }
AND { return ANDOP; }
ANY     { return ANY; }
AS	{ return AS; }
ASC	{ return ASC; }
AUTO_INCREMENT	{ return AUTO_INCREMENT; }
BEFORE	{ return BEFORE; }
<INITIAL>BETWEEN	{ BEGIN BTWMODE; return BETWEEN; }
INT8|BIGINT	{ return BIGINT; }
BINARY	{ return BINARY; }
BIT	{ return BIT; }
BLOB	{ return BLOB; }
BOTH	{ return BOTH; }
BY	{ return BY; }
CALL	{ return CALL; }
CASCADE	{ return CASCADE; }
CASE	{ return CASE; }
CHANGE	{ return CHANGE; }
CHAR(ACTER)?	{ return CHAR; }
CHECK	{ return CHECK; }
COLLATE	{ return COLLATE; }
COLUMN	{ return COLUMN; }
COMMENT	{ return COMMENT; }
CONDITION	{ return CONDITION; }
CONSTRAINT	{ return CONSTRAINT; }
CONTINUE	{ return CONTINUE; }
CONVERT	{ return CONVERT; }
CREATE	{ return CREATE; }
CROSS	{ return CROSS; }
CURRENT { return CURRENT;}
CURRENT_DATE	{ return CURRENT_DATE; }
CURRENT_TIME	{ return CURRENT_TIME; }
CURRENT_TIMESTAMP	{ return CURRENT_TIMESTAMP; }
CURRENT_USER	{ return CURRENT_USER; }
CURSOR	{ return CURSOR; }
DATABASE	{ return DATABASE; }
DATABASES	{ return DATABASES; }
DATE	{ return DATE; }
DATETIME	{ return DATETIME; }
DAY_HOUR	{ return DAY_HOUR; }
DAY_MICROSECOND	{ return DAY_MICROSECOND; }
DAY_MINUTE	{ return DAY_MINUTE; }
DAY_SECOND	{ return DAY_SECOND; }
NUMERIC|DEC|DECIMAL	{ return DECIMAL; }
DECLARE	{ return DECLARE; }
DEFAULT	{ return DEFAULT; }
DELAYED	{ return DELAYED; }
DELETE	{ return DELETE; }
DESC	{ return DESC; }
DESCRIBE	{ return DESCRIBE; }
DETERMINISTIC	{ return DETERMINISTIC; }
DISTINCT	{ return DISTINCT; }
DISTINCTROW	{ return DISTINCTROW; }
DIV	{ return DIV; }
FLOAT8|DOUBLE	{ return DOUBLE; }
DROP	{ return DROP; }
DUAL	{ return DUAL; }
EACH	{ return EACH; }
ELSE	{ return ELSE; }
ELIF	{ return ELSEIF; }
ELSEIF	{ return ELSEIF; }
<FUN_INDENT>END {return FUNDEFEND;}
END	{return END; }
ENUM { return ENUM; }
ESCAPED	{ return ESCAPED; }
EXISTS	{ return EXISTS; }
EXIT	{ return EXIT; }
EXPLAIN	{ return EXPLAIN; }
FETCH	{ return FETCH; }
FLOAT4|FLOAT?	{ return FLOAT; }
FOLLOWING   {return FOLLOWING;  }
FOR	{ return FOR; }
FORCE	{ return FORCE; }
FOREIGN	{ return FOREIGN; }
FROM	{ return FROM; }
FULLTEXT	{ return FULLTEXT; }
GRANT	{ return GRANT; }
GROUP	{ return GROUP; }
HAVING	{ return HAVING; }
HIGH_PRIORITY	{ return HIGH_PRIORITY; }
HOUR_MICROSECOND	{ return HOUR_MICROSECOND; }
HOUR_MINUTE	{ return HOUR_MINUTE; }
HOUR_SECOND	{ return HOUR_SECOND; }
IF	{ return IF; }
IGNORE	{ return IGNORE; }
IN	{ return IN; }
INFILE	{ return INFILE; }
INNER	{ return INNER; }
INOUT	{ return INOUT; }
INSENSITIVE	{ return INSENSITIVE; }
INSERT	{ return INSERT; }
INT4?|INTEGER	{ return INTEGER; }
INTERVAL	{ return INTERVAL; }
INTO	{ return INTO; }
IS	{ return IS; }
ITERATE	{ return ITERATE; }
JOIN	{ return JOIN; }
INDEX   { return INDEX; }
KEY	{ return KEY; }
KEYS	{ return KEYS; }
KILL	{ return KILL; }
LEADING	{ return LEADING; }
LEAVE	{ return LEAVE; }
LEFT	{ return LEFT; }
LIKE	{ return LIKE; }
LIST    { return LIST; }
LIMIT	{ return LIMIT; }
LINES	{ return LINES; }
LOAD	{ return LOAD; }
LOCALTIME	{ return LOCALTIME; }
LOCALTIMESTAMP	{ return LOCALTIMESTAMP; }
LOCK	{ return LOCK; }
LONG	{ return LONG; }
LONGBLOB	{ return LONGBLOB; }
LONGTEXT	{ return LONGTEXT; }
LOOP	{ return LOOP; }
LOW_PRIORITY	{ return LOW_PRIORITY; }
MATCH	{ return MATCH; }
MAP     { return MAP; }
MEDIUMBLOB	{ return MEDIUMBLOB; }
MIDDLEINT|MEDIUMINT	{ return MEDIUMINT; }
MEDIUMTEXT	{ return MEDIUMTEXT; }
MINUTE_MICROSECOND	{ return MINUTE_MICROSECOND; }
MINUTE_SECOND	{ return MINUTE_SECOND; }
MOD	{ return MOD; }
MODIFIES	{ return MODIFIES; }
NATURAL	{ return NATURAL; }
NOT	{ return NOT; }
NO_WRITE_TO_BINLOG	{ return NO_WRITE_TO_BINLOG; }
NULL	{ return NULLX; }
NUMBER	{ return NUMBER; }
ON	{ return ON; }
ON[ \t\n]+DUPLICATE { return ONDUPLICATE; } /* hack due to limited lookahead */
OPTIMIZE	{ return OPTIMIZE; }
OPTION	{ return OPTION; }
OPTIONALLY	{ return OPTIONALLY; }
OR { return OR; }
ORDER	{ return ORDER; }
OUT	{ return OUT; }
OUTER	{ return OUTER; }
OUTFILE	{ return OUTFILE; }
OVER    { return OVER; }
RANGE   {return RANGE;  }
ROW   {return ROW;  }
ROWS   {return ROWS;  }
PARTITION   { return PARTITION; }
PRECISION	{ return PRECISION; }
PRECEDING   {return PRECEDING;  }
PRIMARY	{ return PRIMARY; }
PROCEDURE	{ return PROCEDURE; }
PURGE	{ return PURGE; }
QUICK	{ return QUICK; }
READ	{ return READ; }
READS	{ return READS; }
REAL	{ return REAL; }
REFERENCES	{ return REFERENCES; }
REGEXP|RLIKE	{ return REGEXP; }
RELEASE	{ return RELEASE; }
REPEAT	{ return REPEAT; }
REPLACE	{ return REPLACE; }
REQUIRE	{ return REQUIRE; }
RESTRICT	{ return RESTRICT; }
RETURN	{ return RETURN; }
REVOKE	{ return REVOKE; }
RIGHT	{ return RIGHT; }
ROLLUP	{ return ROLLUP; }
SCHEMA	{ return SCHEMA; }
SCHEMAS	{ return SCHEMAS; }
SECOND_MICROSECOND	{ return SECOND_MICROSECOND; }
SELECT	{ return SELECT; }
SENSITIVE	{ return SENSITIVE; }
SEPARATOR	{ return SEPARATOR; }
SET	{ return SET; }
SHOW	{ return SHOW; }
INT2|SMALLINT	{ return SMALLINT; }
SOME	{ return SOME; }
SPATIAL	{ return SPATIAL; }
SPECIFIC	{ return SPECIFIC; }
SQL	{ return SQL; }
SQLEXCEPTION	{ return SQLEXCEPTION; }
SQLSTATE	{ return SQLSTATE; }
SQLWARNING	{ return SQLWARNING; }
SQL_BIG_RESULT	{ return SQL_BIG_RESULT; }
SQL_CALC_FOUND_ROWS	{ return SQL_CALC_FOUND_ROWS; }
SQL_SMALL_RESULT	{ return SQL_SMALL_RESULT; }
SSL	{ return SSL; }
STRING  {   return STRINGTYPE;}
STARTING	{ return STARTING; }
STRAIGHT_JOIN	{ return STRAIGHT_JOIN; }
TABLE	{ return TABLE; }
TABLES { return TABLES; }
TEMPORARY	{ return TEMPORARY; }
TERMINATED	{ return TERMINATED; }
TEXT	{ return TEXT; }
THEN	{ return THEN; }
TIME	{ return TIME; }
TIMESTAMP	{ return TIMESTAMP; }
INT1|TINYINT	{ return TINYINT; }
TINYTEXT	{ return TINYTEXT; }
TO	{ return TO; }
TS  { return TS; }
TTL  { return TTL; }
TRAILING	{ return TRAILING; }
TRIGGER	{ return TRIGGER; }
UNDO	{ return UNDO; }
UNION	{ return UNION; }
UNIQUE	{ return UNIQUE; }
UNLOCK	{ return UNLOCK; }
UNSIGNED	{ return UNSIGNED; }
UNBOUNDED   {return UNBOUNDED;  }
UPDATE	{ return UPDATE; }
USAGE	{ return USAGE; }
USE	{ return USE; }
USING	{ return USING; }
UTC_DATE	{ return UTC_DATE; }
UTC_TIME	{ return UTC_TIME; }
UTC_TIMESTAMP	{ return UTC_TIMESTAMP; }
VALUES?	{ return VALUES; }
FUN_IDENTIFIER     { return FUN_IDENTIFIER; }
VARBINARY	{ return VARBINARY; }
VARCHAR(ACTER)?	{ return VARCHAR; }
VARYING	{ return VARYING; }
VERSION { return VERSION; }
WHEN	{ return WHEN; }
WHERE	{ return WHERE; }
WHILE	{ return WHILE; }
WITH	{ return WITH; }
WRITE	{ return WRITE; }
WINDOW  { return WINDOW; }
XOR	{ return XOR; }
YEAR	{ return YEAR; }
YEAR_MONTH	{ return YEAR_MONTH; }
ZEROFILL	{ return ZEROFILL; }


 /* function definition */
<FUN_INDENT>"def" {
            BEGIN FUN;
            return DEF;
       }

 /* type definition */
<FUN>"i32" {
    yylval->intval = 1;
    return I32;
    }


<FUN>{newlines} { columnpos = 1; BEGIN FUN_INDENT; return NEWLINES;}
<FUN_INDENT>{newlines} { columnpos = 1; BEGIN FUN_INDENT; return NEWLINES;}

<FUN_INDENT>{indent} {
        int space_cnt = 0;
        for(int i=0; i < static_cast<int>(strlen(yytext)); i++) {
            if (yytext[i] == '\t') {
                space_cnt += 4;
            } else {
                space_cnt += 1;
            }
        }
        yylval->intval = space_cnt;
        BEGIN FUN;
        return INDENT;
}


 /* operator */

 /* numbers */
-?[0-9]+	        { yylval->intval = atoi(yytext); return INTNUM; }
-?[0-9]+['d']	        { yylval->intval = atoi(yytext); return DAYNUM; }
-?[0-9]+['h']	        { yylval->intval = atoi(yytext); return HOURNUM; }
-?[0-9]+['m']	        { yylval->intval = atoi(yytext); return MINUTENUM; }
-?[0-9]+['s']	        { yylval->intval = atoi(yytext); return SECONDNUM; }

-?[0-9]+"."[0-9]* |
-?"."[0-9]+	|
-?[0-9]+E[-+]?[0-9]+	|
-?[0-9]+"."[0-9]*E[-+]?[0-9]+ |
-?"."[0-9]*E[-+]?[0-9]+	{ yylval->floatval = atof(yytext) ;
                                  return APPROXNUM; }
 /* booleans */
TRUE	{ yylval->intval = 1; return BOOL; }
UNKNOWN	{ yylval->intval = -1; return BOOL; }
FALSE	{ yylval->intval = 0; return BOOL; }


'(\\.|''|[^'\n])*'	|
\"(\\.|\"\"|[^"\n])*\"  {
                            char *src = yytext+1;
                            int len = strlen(src) - 1; //remove last quote charactor
                            yylval->strval = (char*)(malloc(len+1));
                            parse_string(src, yylval->strval, len, 1);
                            return STRING;
                         }

'(\\.|[^'\n])*$	        { yyerror("Unterminated string %s", yytext); }
\"(\\.|[^"\n])*$	{ yyerror("Unterminated string %s", yytext); }

   /* hex strings */
X'[0-9A-F]+' |
0X[0-9A-F]+  { yylval->strval = strdup(yytext); return STRING; }

 /* bit strings */

0B[01]+      |
B'[01]+'     { yylval->strval = strdup(yytext); return STRING; }


 /* operators */

{and}	{ return ANDOP; }
{or}	{ return OR; }
{less_equals}	{
					return LESS_EQUALS;
				}

{greater_equals} {
					return GREATER_EQUALS;
				}

{not_equals}	{
					/* We accept both "<>" and "!=" as meaning NOT_EQUALS */
					return NOT_EQUALS;
				}

<INITIAL>":="     { return ASSIGN; }
<FUN>{assign} { return ASSIGN; }
<FUN>{add_assign} { return ADD_ASSIGN; }
<FUN>{minus_assign} { return MINUS_ASSIGN; }
<FUN>{multi_assign} { return MULTI_ASSIGN; }
<FUN>{fdiv_assign} { return FDIV_ASSIGN; }
<INITIAL>{assign} { return EQUALS; }
{equals} { return EQUALS; }
<FUN>[A-Za-z][A-Za-z0-9_]*	{ yylval->strval = strdup(yytext);
                          return FUN_IDENTIFIER; }

<FUN>`[^`/\\.\n]+`           { yylval->strval = strdup(yytext+1);
                          yylval->strval[yyleng-2] = 0;
                          return FUN_IDENTIFIER; }

[A-Za-z][A-Za-z0-9_]*	{ yylval->strval = strdup(yytext);
                          return SQL_IDENTIFIER; }
`[^`/\\.\n]+`           { yylval->strval = strdup(yytext+1);
                          yylval->strval[yyleng-2] = 0;
                          return SQL_IDENTIFIER; }

`[^`\n]*$               { yyerror("unterminated quoted name %s", yytext); }

@[0-9a-z_.$]+ |
@\"[^"\n]+\" |
@`[^`\n]+` |
@'[^'\n]+' { yylval->strval = strdup(yytext+1); return USERVAR; }

@\"[^"\n]*$ { yyerror("unterminated quoted user variable %s", yytext); }
@`[^`\n]*$ { yyerror("unterminated quoted user variable %s", yytext); }
@'[^'\n]*$ { yyerror("unterminated quoted user variable %s", yytext); }



<FUN>{identifier} {
            yylval->strval = strdup(yytext);
            return FUN_IDENTIFIER;
    }

{self} {
            return yytext[0];
	}

[-+&~|^/%*(),.;!]   { return yytext[0]; }
#.*		;
"--"[ \t].*	;

"/*"            { BEGIN COMMENT; }
<COMMENT>"*/"   { BEGIN INITIAL; }
<COMMENT>.|\n   ;
<COMMENT><<EOF>> { yyerror("unclosed comment"); }


	/* everything else */
[ \n\t]         /* white space */{}
.               { yyerror("mystery character '%s'", *yytext); }

%%


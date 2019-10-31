%option noinput nounput noyywrap 8bit nodefault
%option yylineno
%option reentrant bison-bridge bison-locations
%option outfile="fn_lexer.gen.cc"
%{
#include "ast/fn_parser.gen.h"
static int process_integer_literal(const char *token, YYSTYPE *lval);
%}

space [ \t]
newline [\n\r]
non_newline [^\n\r]
comment ("#"{non_newline}*)
digit [0-9]
ident_start [A-Za-z\200-\377_]
ident_cont [A-Za-z\200-\377_0-9\$]
identifier {ident_start}{ident_cont}*

self [,()\[\].;\:\+\-\*\/\%\^\<\>\=]
op_chars [\~\NEWLINE!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=]
operator {op_chars}+
integer {digit}+
decimal (({digit}*\.{digit}+)|({digit}+\.{digit}*))
indent {space}*

%%

"def" {
    return DEF;
    }

"return" {
    return RETURN;
    }

"i32" {
    yylval->ival = 1;
    return I32;
    }

{space} {return SPACE;}
{newline} {return NEWLINE;}

{indent} {
    yylval->ival = strlen(yytext);
    return INDENT;
}

{integer} {
		process_integer_literal(yytext, yylval);
        return INTEGER;
    }
{identifier} {
        std::string id(yytext);
        yylval->sval = id;
        return IDENTIFIER;
    }
{self} {
        return yytext[0];
	}
.	{printf("bad input character '%s' at line %d\n", yytext, yylineno);}
%%

static int
process_integer_literal(const char *token, YYSTYPE *lval){
	int	val = atoi(token);
	lval->ival = val;
	return INTEGER;
}


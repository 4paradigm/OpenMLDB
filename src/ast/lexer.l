%option noinput nounput noyywrap 8bit nodefault                                 
%option yylineno
%option reentrant bison-bridge bison-locations 
%option outfile="lexer.gen.cc"
%{
#include "ast/parser.gen.h"
%}

%%

"=" {return ASSIGN;}
\n {return NEWLINE;}
"(" {return LBK;}
")" {return RBK;}

[a-zA-Z_][a-zA-Z0-9_]* { yylval->sval = strdup(yytext); return ID;}
.	{printf("bad input character '%s' at line %d\n", yytext, yylineno);}

%%

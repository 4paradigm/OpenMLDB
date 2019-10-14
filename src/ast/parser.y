%define api.pure full
%locations

%{
#include <stdlib.h>
#include "ast/parser.gen.h"
%}

%output  "parser.gen.cc"
%defines "parser.gen.h"

// Prefix the parser
%define parse.error verbose
%locations
%lex-param   { yyscan_t scanner }

%parse-param { yyscan_t scanner }
%parse-param { ::cxx::ast::TreeNode* root }


%code requires {
#include "ast/ast.h"

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif
}

%union {
  struct ::cxx::ast::TreeNode* node;
  int32_t ival;
  int64_t lval;
  float fval;
  double dval;
  char* sval;
}

%{

extern int yylex(YYSTYPE* yylvalp, YYLTYPE* yyllocp, yyscan_t scanner);
void yyerror(YYLTYPE* yyllocp, yyscan_t unused, ::cxx::ast::TreeNode* root, const char* msg ) {
	printf("error %s", msg);
}

%}


%token <sval> ID
%token <sval> ASSIGN
%token <sval> NEWLINE
%token <sval> LBK
%token <sval> RBK
%token EOL

%type <node> stmt_list stmt var fncall plist
%start stmt_list 

%%
stmt_list: stmt  
   | stmt NEWLINE stmt_list
;

stmt : var ASSIGN fncall {
       $$ = new ::cxx::ast::TreeNode();
       $$->type = ::cxx::ast::kStmt;
	   root->children.push_back($$);
       $$->children.push_back($1);
       $$->children.push_back($3);
     }
     ;

var : ID {
       ::cxx::ast::VarNode*  vn = new ::cxx::ast::VarNode();
       vn->type = ::cxx::ast::kVar;
       vn->name = $1;
       $$ = (::cxx::ast::TreeNode*)vn;
     }
    ;

fncall : ID LBK plist RBK
       {
        ::cxx::ast::FnCallNode* fn = new ::cxx::ast::FnCallNode();
        fn->name = $1;
        fn->type = ::cxx::ast::kFnCall;
        fn->children.push_back($3);
        $$ = (::cxx::ast::TreeNode*)fn;
       }
       ;

plist: ID  {
        ::cxx::ast::FnCallParaNode* pn = new ::cxx::ast::FnCallParaNode();
        pn->type = ::cxx::ast::kFnParaList;
        pn->children.push_back($1);
        $$ = (::cxx::ast::TreeNode*)pn;
     }
     | ID ',' plist {
        ::cxx::ast::FnCallParaNode* pn = (::cxx::ast::FnCallParaNode*)$3;
        pn->children.push_back($1);
        $$ = $3;
     }
     ;
%%



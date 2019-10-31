%define api.pure full
%locations

%{
#include <stdlib.h>
#include <stdio.h>
#include "ast/fn_parser.gen.h"

%}

%output  "fn_parser.gen.cc"
%defines "fn_parser.gen.h"

// Prefix the parser
%define parse.error verbose
%locations
%lex-param   { yyscan_t scanner }
%parse-param { yyscan_t scanner }
%parse-param { ::fesql::node::FnNode* root }

%code requires {
#include "ast/fn_ast.h"

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

}

%union {
  struct ::fesql::node::FnNode* node;
  int32_t ival;
  int64_t lval;
  float fval;
  double dval;
  char* sval;
}

%{

extern int fnlex(YYSTYPE* ylvalp, YYLTYPE* yyllocp, yyscan_t scanner);
void yyerror(YYLTYPE* yyllocp, 
            yyscan_t unused, ::fesql::node::FnNode* root, const char* msg ) {
	printf("error %s", msg);
}

%}

%right '='
%left '+' '-'
%left '*' '/'

%token <ival> I32
%token <sval> IDENTIFIER
%token <sval> NEWLINE
%token <ival> INDENT
%token <sval> DEF
%token <sval> RETURN
%token <sval> SPACE_REQUIRED
%token <sval> SPACE
%token <ival> INTEGER
%token EOL

%type <node> grammar line_list primary var types
             indented fn_def return_stmt assign_stmt para plist expr
%start grammar

%%

grammar : line_list;

line_list: indented 
         | indented NEWLINE line_list;

indented :
         fn_def {
            $1->indent = 0;
            root->children.push_back($1);
         }
         | 
         INDENT fn_def {
            $2->indent = $1;
            root->children.push_back($2);
         }
         | return_stmt {
            $1->indent = 0;
            root->children.push_back($1);
         }
         | INDENT return_stmt {
            $2->indent = $1;
            root->children.push_back($2);
         }
         | INDENT assign_stmt {
            $2->indent = $1;
            root->children.push_back($2);
         };

fn_def : 
       DEF SPACE  IDENTIFIER '(' plist ')' ':' types {
            ::fesql::node::FnNodeFnDef* fn_def = new ::fesql::node::FnNodeFnDef();
            fn_def->type = ::fesql::node::kFnDef;
            fn_def->name = $3;
            fn_def->children.push_back($5);
            fn_def->ret_type = $8->type;
            $$ = (::fesql::node::FnNode*) fn_def;
       };

assign_stmt: IDENTIFIER '=' expr {
            ::fesql::node::FnAssignNode* fn_assign = new ::fesql::node::FnAssignNode();
            fn_assign->type = ::fesql::node::kFnAssignStmt;
            fn_assign->name = $1;
            fn_assign->children.push_back($3);
            $$ = (::fesql::node::FnNode*) fn_assign;
           };

return_stmt:
           RETURN SPACE  expr {
            $$ = new ::fesql::node::FnNode();
            $$->type = ::fesql::node::kFnReturnStmt;
            $$->children.push_back($3);
           };

types: I32 {
        $$ = new ::fesql::node::FnNode();
        $$->type = ::fesql::node::kFnPrimaryInt32;
     };

plist:
     para {
        $$ = new ::fesql::node::FnNode();
        $$->type = ::fesql::node::kFnParaList;
        $$->children.push_back($1);
     } | para ',' plist  {
        $3->children.push_back($1);
        $$ = $3;
     }; 

para: IDENTIFIER ':' types {
        ::fesql::node::FnParaNode* para_node = new ::fesql::node::FnParaNode();
        para_node->type = ::fesql::node::kFnPara;
        para_node->name = $1;
        para_node->para_type = $3->type;
        $$ = (::fesql::node::FnNode*) para_node;
    };
primary: INTEGER {
       ::fesql::node::FnNodeInt32* i32_node = new ::fesql::node::FnNodeInt32();
        i32_node->type = ::fesql::node::kFnPrimaryInt32;
        i32_node->value = $1;
        $$ = (::fesql::node::FnNode*)i32_node;
    };
var: IDENTIFIER {
        ::fesql::node::FnIdNode* id_node = new ::fesql::node::FnIdNode();
        id_node->type = ::fesql::node::kFnId;
        id_node->name = $1;
        $$ = (::fesql::node::FnNode*)id_node;
     };

expr : expr '+' expr {
        ::fesql::node::FnBinaryExpr* bexpr = new ::fesql::node::FnBinaryExpr();
        bexpr->type = ::fesql::node::kFnExprBinary;
        bexpr->children.push_back($1);
        bexpr->children.push_back($3);
        bexpr->op = ::fesql::node::kFnOpAdd;
        $$ = (::fesql::node::FnNode*)bexpr;
     }
     | expr '-' expr {
        ::fesql::node::FnBinaryExpr* bexpr = new ::fesql::node::FnBinaryExpr();
        bexpr->type = ::fesql::node::kFnExprBinary;
        bexpr->children.push_back($1);
        bexpr->children.push_back($3);
        bexpr->op = ::fesql::node::kFnOpMinus;
        $$ = (::fesql::node::FnNode*)bexpr;
     }
     | expr '*' expr {
        ::fesql::node::FnBinaryExpr* bexpr = new ::fesql::node::FnBinaryExpr();
        bexpr->type = ::fesql::node::kFnExprBinary;
        bexpr->children.push_back($1);
        bexpr->children.push_back($3);
        bexpr->op = ::fesql::node::kFnOpMulti;
        $$ = (::fesql::node::FnNode*)bexpr;
     }
     | expr '/' expr {
        ::fesql::node::FnBinaryExpr* bexpr = new ::fesql::node::FnBinaryExpr();
        bexpr->type = ::fesql::node::kFnExprBinary;
        bexpr->children.push_back($1);
        bexpr->children.push_back($3);
        bexpr->op = ::fesql::node::kFnOpDiv;
        $$ = (::fesql::node::FnNode*)bexpr;
     }
     | '(' expr ')' {
        ::fesql::node::FnUnaryExpr* uexpr = new ::fesql::node::FnUnaryExpr();
        uexpr->type = ::fesql::node::kFnExprUnary;
        uexpr->op = ::fesql::node::kFnOpBracket;
        uexpr->children.push_back($2);
        $$ = (::fesql::node::FnNode*)uexpr;
     }
     | primary 
     | var;
%%



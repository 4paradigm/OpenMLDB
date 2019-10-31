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
%parse-param { ::fesql::node::NodeManager *node_manager}

%code requires {
#include "ast/fn_ast.h"
#include "node/node_memory.h"

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
            yyscan_t unused, ::fesql::node::FnNode* root, ::fesql::node::NodeManager *node_manager, const char* msg ) {
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
            root->AddChildren($1);
         }
         | 
         INDENT fn_def {
            $2->indent = $1;
            root->AddChildren($2);
         }
         | return_stmt {
            $1->indent = 0;
            root->AddChildren($1);
         }
         | INDENT return_stmt {
            $2->indent = $1;
            root->AddChildren($2);
         }
         | INDENT assign_stmt {
            $2->indent = $1;
            root->AddChildren($2);
         };

fn_def : 
       DEF SPACE  IDENTIFIER '(' plist ')' ':' types {
            $$ = node_manager->MakeFnDefNode($3, $5, $8->type);
       };

assign_stmt: IDENTIFIER '=' expr {
            $$ = node_manager->MakeAssignNode($1, $3);
           };

return_stmt:
           RETURN SPACE  expr {
            $$ = node_manager->MakeReturnStmtNode($3);
           };

types: I32 {
            $$ = node_manager->MakeTypeNode(::fesql::node::kTypeInt32);
           }
       ;

plist:
     para {
        $$ = node_manager->MakeFnNode(::fesql::node::kFnParaList);
        $$->AddChildren($1);
     } | para ',' plist  {
        $3->AddChildren($1);
        $$ = $3;
     }; 

para: IDENTIFIER ':' types {
        $$ = node_manager->MakeFnParaNode($1, $3->type);
    };

primary: INTEGER {
       ::fesql::node::FnNodeInt32* i32_node = new ::fesql::node::FnNodeInt32();
        i32_node->type = ::fesql::node::kInt;
        i32_node->value = $1;
        $$ = (::fesql::node::FnNode*)i32_node;
    };
var: IDENTIFIER {
        $$ = node_manager->MakeFnIdNode($1);
     };

expr : expr '+' expr {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpAdd);
     }
     | expr '-' expr {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpMinus);
     }
     | expr '*' expr {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpMulti);
     }
     | expr '/' expr {
        $$ = node_manager->MakeBinaryExprNode($1, $3, ::fesql::node::kFnOpDiv);
     }
     | '(' expr ')' {
        $$ = node_manager->MakeUnaryExprNode($2, ::fesql::node::kFnOpBracket);
     }
     | primary 
     | var;
%%



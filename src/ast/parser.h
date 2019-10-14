#include "ast/ast.h"
#include "ast/parser.gen.h"

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

#ifndef YY_TYPEDEF_YY_BUFFER_STATE
#define YY_TYPEDEF_YY_BUFFER_STATE
typedef struct yy_buffer_state *YY_BUFFER_STATE;
#endif

extern YY_BUFFER_STATE yy_scan_string ( const char *yy_str , yyscan_t yyscanner );
extern int yylex_init (yyscan_t* scanner);
extern int yyparse (yyscan_t scanner, ::cxx::ast::TreeNode* root);
extern int yylex_destroy ( yyscan_t yyscanner );

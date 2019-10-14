#include <stdio.h>
#include "ast/parser.h"

int main(int argc, char** args) {
	yyscan_t scan;
	int ret0 = yylex_init(&scan);
	const char* test ="a=discrete(xxxx)\nb=discrete(xxxx)";
	yy_scan_string(test, scan);
	::fedb::ast::TreeNode node;
	int ret = yyparse(scan, &node);
	printf("parse ret %d\n", ret);
	printf("child cnt %lu\n", node.children.size());
	return 0;
}



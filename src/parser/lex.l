/*词法分析器*/
    /* keywords are case insensitive */
/* 关键字不区分大小写（SELECT = select */
%option caseless
    /* we don't need yywrap() function */
/* 不需要处理多文件 */
%option noyywrap
    /* we don't need yyunput() function */
/* 不需要退回字符功能 */
%option nounput
    /* we don't need input() function */
/* 不需要输入功能 */
%option noinput
    /* enable location */
/* 与语法分析器配合工作 */
%option bison-bridge
/* 记录每个词的位置 */
%option bison-locations

%{
#include "ast.h"
#include "yacc.tab.h"
#include <iostream>

// 自动更新位置信息的宏
#define YY_USER_ACTION \
    yylloc->first_line = yylloc->last_line; \
    yylloc->first_column = yylloc->last_column; \
    for (int i = 0; yytext[i] != '\0'; i++) { \
        if(yytext[i] == '\n') { \
            yylloc->last_line++; \
            yylloc->last_column = 1; \
        } else { \
            yylloc->last_column++; \
        } \
    }

%}

alpha [a-zA-Z]
digit [0-9]
white_space [ \t]+
new_line "\r"|"\n"|"\r\n"
sign "+"|"-"
identifier {alpha}(_|{alpha}|{digit})*
value_int {sign}?{digit}+
value_float {sign}?{digit}+\.({digit}+)?
value_string '[^']*'
single_op ";"|"("|")"|","|"*"|"="|">"|"<"|"."|"+"|"-"|"/"

%x STATE_COMMENT

%%
    /* block comment */
"/*" { BEGIN(STATE_COMMENT); }
<STATE_COMMENT>"*/" { BEGIN(INITIAL); }
<STATE_COMMENT>[^*] { /* ignore the text of the comment */ }
<STATE_COMMENT>\* { /* ignore *'s that aren't part of */ }
    /* single line comment */
"--".* { /* ignore single line comment */ }
    /* white space and new line */
{white_space} { /* ignore white space */ }
{new_line} { /* ignore new line */ }
    /* keywords */
"SHOW" { return SHOW; }
"BEGIN" { return TXN_BEGIN; }
"COMMIT" { return TXN_COMMIT; }
"ABORT" { return TXN_ABORT; }
"ROLLBACK" { return TXN_ROLLBACK; }
"TABLES" { return TABLES; }
"CREATE" { return CREATE; }
"TABLE" { return TABLE; }
"DROP" { return DROP; }
"DESC" { return DESC; }
"INSERT" { return INSERT; }
"INTO" { return INTO; }
"VALUES" { return VALUES; }
"DELETE" { return DELETE; }
"FROM" { return FROM; }
"WHERE" { return WHERE; }
"UPDATE" { return UPDATE; }
"SET" { return SET; }
"SELECT" { return SELECT; }
"INT" { return INT; }
"CHAR" { return CHAR; }
"FLOAT" { return FLOAT; }
"INDEX" { return INDEX; }
"AND" { return AND; }
"JOIN" { return JOIN; }
"INNER_JOIN" { return INNER_JOIN; }
"LEFT_JOIN" { return LEFT_JOIN; }
"RIGHT_JOIN" { return RIGHT_JOIN; }
"FULL_JOIN" { return FULL_JOIN; }
"SEMI" { return SEMI; }
"ON" { return ON; }
"EXIT" { return EXIT; }
"HELP" { return HELP; }
"ORDER" { return ORDER; }
"GROUP" { return GROUP; }
"BY" {  return BY;  }
"HAVING" { return HAVING; }
"ASC" { return ASC; }
"COUNT" { return COUNT; }
"SUM" { return SUM; }
"AVG" { return AVG; }
"MIN" { return MIN; }
"MAX" { return MAX; }
"AS" { return AS; }
"LIMIT" { return LIMIT; }
"OFFSET" { return OFFSET; }
"EXPLAIN" { return EXPLAIN; }
"ENABLE_NESTLOOP" { return ENABLE_NESTLOOP; }
"ENABLE_SORTMERGE" { return ENABLE_SORTMERGE; }
"STATIC_CHECKPOINT" { return STATIC_CHECKPOINT; }
"TRUE" { 
    yylval->sv_bool = true;
    return VALUE_BOOL; 
}
"FALSE" {
    yylval->sv_bool = false;
    return VALUE_BOOL;
}
    /* operators */
">=" { return GEQ; }
"<=" { return LEQ; }
"<>" { return NEQ; }
{single_op} { return yytext[0]; }
    /* id */
{identifier} {
    yylval->sv_str = yytext;
    return IDENTIFIER;
}
    /* literals */
{value_int} {
    yylval->sv_int = atoi(yytext);
    return VALUE_INT;
}
{value_float} {
    yylval->sv_float = atof(yytext);
    return VALUE_FLOAT;
}
{value_string} {
    yylval->sv_str = std::string(yytext + 1, strlen(yytext) - 2);
    return VALUE_STRING;
}
    /* EOF */
<<EOF>> { return T_EOF; }
    /* unexpected char */
. { std::cerr << "Lexer Error: unexpected character " << yytext[0] << std::endl; }
%%

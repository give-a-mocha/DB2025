/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 1 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"


#include "ast.h"
#include "yacc.tab.h"
#include <iostream>
#include <memory>

// 词法分析器接口函数声明
int yylex(YYSTYPE *yylval, YYLTYPE *yylloc);

// 语法错误处理函数
void yyerror(YYLTYPE *locp, const char* s) {
    std::cerr << "Parser Error at line " << locp->first_line << " column " << locp->first_column << ": " << s << std::endl;
}

using namespace ast;

#line 89 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "yacc.tab.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_SHOW = 3,                       /* SHOW  */
  YYSYMBOL_TABLES = 4,                     /* TABLES  */
  YYSYMBOL_CREATE = 5,                     /* CREATE  */
  YYSYMBOL_TABLE = 6,                      /* TABLE  */
  YYSYMBOL_DROP = 7,                       /* DROP  */
  YYSYMBOL_DESC = 8,                       /* DESC  */
  YYSYMBOL_INSERT = 9,                     /* INSERT  */
  YYSYMBOL_INTO = 10,                      /* INTO  */
  YYSYMBOL_VALUES = 11,                    /* VALUES  */
  YYSYMBOL_DELETE = 12,                    /* DELETE  */
  YYSYMBOL_FROM = 13,                      /* FROM  */
  YYSYMBOL_ASC = 14,                       /* ASC  */
  YYSYMBOL_ORDER = 15,                     /* ORDER  */
  YYSYMBOL_GROUP = 16,                     /* GROUP  */
  YYSYMBOL_BY = 17,                        /* BY  */
  YYSYMBOL_HAVING = 18,                    /* HAVING  */
  YYSYMBOL_COUNT = 19,                     /* COUNT  */
  YYSYMBOL_SUM = 20,                       /* SUM  */
  YYSYMBOL_AVG = 21,                       /* AVG  */
  YYSYMBOL_MIN = 22,                       /* MIN  */
  YYSYMBOL_MAX = 23,                       /* MAX  */
  YYSYMBOL_WHERE = 24,                     /* WHERE  */
  YYSYMBOL_UPDATE = 25,                    /* UPDATE  */
  YYSYMBOL_SET = 26,                       /* SET  */
  YYSYMBOL_SELECT = 27,                    /* SELECT  */
  YYSYMBOL_INT = 28,                       /* INT  */
  YYSYMBOL_CHAR = 29,                      /* CHAR  */
  YYSYMBOL_FLOAT = 30,                     /* FLOAT  */
  YYSYMBOL_INDEX = 31,                     /* INDEX  */
  YYSYMBOL_AND = 32,                       /* AND  */
  YYSYMBOL_JOIN = 33,                      /* JOIN  */
  YYSYMBOL_EXIT = 34,                      /* EXIT  */
  YYSYMBOL_HELP = 35,                      /* HELP  */
  YYSYMBOL_TXN_BEGIN = 36,                 /* TXN_BEGIN  */
  YYSYMBOL_TXN_COMMIT = 37,                /* TXN_COMMIT  */
  YYSYMBOL_TXN_ABORT = 38,                 /* TXN_ABORT  */
  YYSYMBOL_TXN_ROLLBACK = 39,              /* TXN_ROLLBACK  */
  YYSYMBOL_ORDER_BY = 40,                  /* ORDER_BY  */
  YYSYMBOL_ENABLE_NESTLOOP = 41,           /* ENABLE_NESTLOOP  */
  YYSYMBOL_ENABLE_SORTMERGE = 42,          /* ENABLE_SORTMERGE  */
  YYSYMBOL_EXPLAIN = 43,                   /* EXPLAIN  */
  YYSYMBOL_AS = 44,                        /* AS  */
  YYSYMBOL_INNER_JOIN = 45,                /* INNER_JOIN  */
  YYSYMBOL_LEFT_JOIN = 46,                 /* LEFT_JOIN  */
  YYSYMBOL_RIGHT_JOIN = 47,                /* RIGHT_JOIN  */
  YYSYMBOL_FULL_JOIN = 48,                 /* FULL_JOIN  */
  YYSYMBOL_ON = 49,                        /* ON  */
  YYSYMBOL_LEQ = 50,                       /* LEQ  */
  YYSYMBOL_NEQ = 51,                       /* NEQ  */
  YYSYMBOL_GEQ = 52,                       /* GEQ  */
  YYSYMBOL_T_EOF = 53,                     /* T_EOF  */
  YYSYMBOL_IDENTIFIER = 54,                /* IDENTIFIER  */
  YYSYMBOL_VALUE_STRING = 55,              /* VALUE_STRING  */
  YYSYMBOL_VALUE_INT = 56,                 /* VALUE_INT  */
  YYSYMBOL_VALUE_FLOAT = 57,               /* VALUE_FLOAT  */
  YYSYMBOL_VALUE_BOOL = 58,                /* VALUE_BOOL  */
  YYSYMBOL_59_ = 59,                       /* ';'  */
  YYSYMBOL_60_ = 60,                       /* '='  */
  YYSYMBOL_61_ = 61,                       /* '('  */
  YYSYMBOL_62_ = 62,                       /* ')'  */
  YYSYMBOL_63_ = 63,                       /* ','  */
  YYSYMBOL_64_ = 64,                       /* '.'  */
  YYSYMBOL_65_ = 65,                       /* '*'  */
  YYSYMBOL_66_ = 66,                       /* '<'  */
  YYSYMBOL_67_ = 67,                       /* '>'  */
  YYSYMBOL_YYACCEPT = 68,                  /* $accept  */
  YYSYMBOL_start = 69,                     /* start  */
  YYSYMBOL_stmt = 70,                      /* stmt  */
  YYSYMBOL_txnStmt = 71,                   /* txnStmt  */
  YYSYMBOL_dbStmt = 72,                    /* dbStmt  */
  YYSYMBOL_setStmt = 73,                   /* setStmt  */
  YYSYMBOL_ddl = 74,                       /* ddl  */
  YYSYMBOL_dml = 75,                       /* dml  */
  YYSYMBOL_fieldList = 76,                 /* fieldList  */
  YYSYMBOL_colNameList = 77,               /* colNameList  */
  YYSYMBOL_field = 78,                     /* field  */
  YYSYMBOL_type = 79,                      /* type  */
  YYSYMBOL_valueList = 80,                 /* valueList  */
  YYSYMBOL_value = 81,                     /* value  */
  YYSYMBOL_condition = 82,                 /* condition  */
  YYSYMBOL_optWhereClause = 83,            /* optWhereClause  */
  YYSYMBOL_whereClause = 84,               /* whereClause  */
  YYSYMBOL_optAlias = 85,                  /* optAlias  */
  YYSYMBOL_col = 86,                       /* col  */
  YYSYMBOL_colList = 87,                   /* colList  */
  YYSYMBOL_op = 88,                        /* op  */
  YYSYMBOL_expr = 89,                      /* expr  */
  YYSYMBOL_setClauses = 90,                /* setClauses  */
  YYSYMBOL_setClause = 91,                 /* setClause  */
  YYSYMBOL_selector = 92,                  /* selector  */
  YYSYMBOL_tableRef = 93,                  /* tableRef  */
  YYSYMBOL_tableList = 94,                 /* tableList  */
  YYSYMBOL_opt_order_clause = 95,          /* opt_order_clause  */
  YYSYMBOL_order_clause = 96,              /* order_clause  */
  YYSYMBOL_opt_asc_desc = 97,              /* opt_asc_desc  */
  YYSYMBOL_opt_group_clause = 98,          /* opt_group_clause  */
  YYSYMBOL_group_clause = 99,              /* group_clause  */
  YYSYMBOL_group_clauses = 100,            /* group_clauses  */
  YYSYMBOL_opt_having_conds = 101,         /* opt_having_conds  */
  YYSYMBOL_having_conds = 102,             /* having_conds  */
  YYSYMBOL_optJoinExprs = 103,             /* optJoinExprs  */
  YYSYMBOL_joinExprs = 104,                /* joinExprs  */
  YYSYMBOL_joinExpr = 105,                 /* joinExpr  */
  YYSYMBOL_joinType = 106,                 /* joinType  */
  YYSYMBOL_set_knob_type = 107,            /* set_knob_type  */
  YYSYMBOL_tbName = 108,                   /* tbName  */
  YYSYMBOL_colName = 109                   /* colName  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_uint8 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if 1

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* 1 */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
             && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE) \
             + YYSIZEOF (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  52
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   224

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  68
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  42
/* YYNRULES -- Number of rules.  */
#define YYNRULES  109
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  231

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   313


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      61,    62,    65,     2,    63,     2,    64,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    59,
      66,    60,    67,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,    99,    99,   104,   109,   114,   123,   124,   125,   126,
     127,   132,   136,   140,   144,   152,   156,   164,   172,   176,
     180,   184,   188,   196,   200,   204,   208,   212,   219,   223,
     231,   235,   243,   251,   255,   259,   267,   271,   279,   283,
     287,   291,   299,   308,   311,   319,   323,   332,   335,   339,
     347,   351,   355,   359,   363,   367,   371,   375,   379,   383,
     387,   391,   395,   403,   407,   415,   419,   423,   427,   431,
     435,   443,   447,   455,   459,   467,   475,   479,   484,   494,
     498,   506,   511,   518,   526,   530,   535,   541,   545,   548,
     553,   557,   563,   564,   570,   574,   584,   587,   595,   599,
     607,   615,   619,   623,   627,   631,   639,   643,   650,   651
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if 1
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "SHOW", "TABLES",
  "CREATE", "TABLE", "DROP", "DESC", "INSERT", "INTO", "VALUES", "DELETE",
  "FROM", "ASC", "ORDER", "GROUP", "BY", "HAVING", "COUNT", "SUM", "AVG",
  "MIN", "MAX", "WHERE", "UPDATE", "SET", "SELECT", "INT", "CHAR", "FLOAT",
  "INDEX", "AND", "JOIN", "EXIT", "HELP", "TXN_BEGIN", "TXN_COMMIT",
  "TXN_ABORT", "TXN_ROLLBACK", "ORDER_BY", "ENABLE_NESTLOOP",
  "ENABLE_SORTMERGE", "EXPLAIN", "AS", "INNER_JOIN", "LEFT_JOIN",
  "RIGHT_JOIN", "FULL_JOIN", "ON", "LEQ", "NEQ", "GEQ", "T_EOF",
  "IDENTIFIER", "VALUE_STRING", "VALUE_INT", "VALUE_FLOAT", "VALUE_BOOL",
  "';'", "'='", "'('", "')'", "','", "'.'", "'*'", "'<'", "'>'", "$accept",
  "start", "stmt", "txnStmt", "dbStmt", "setStmt", "ddl", "dml",
  "fieldList", "colNameList", "field", "type", "valueList", "value",
  "condition", "optWhereClause", "whereClause", "optAlias", "col",
  "colList", "op", "expr", "setClauses", "setClause", "selector",
  "tableRef", "tableList", "opt_order_clause", "order_clause",
  "opt_asc_desc", "opt_group_clause", "group_clause", "group_clauses",
  "opt_having_conds", "having_conds", "optJoinExprs", "joinExprs",
  "joinExpr", "joinType", "set_knob_type", "tbName", "colName", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-145)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-109)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     133,    -2,     1,     3,   -39,    34,    23,   -39,    45,    26,
    -145,  -145,  -145,  -145,  -145,  -145,    13,  -145,    54,     8,
    -145,  -145,  -145,  -145,  -145,  -145,    56,   -39,   -39,   -39,
     -39,  -145,  -145,   -39,   -39,    50,  -145,  -145,    18,    20,
      60,    62,    63,    64,    42,  -145,  -145,    66,   114,    69,
     -16,    26,  -145,  -145,   -39,    76,    78,  -145,    82,   140,
     120,    98,    95,   -42,   100,   100,   100,   100,    -1,   -39,
      98,   103,  -145,  -145,   148,  -145,    98,    98,    98,   101,
      -1,  -145,  -145,   -13,  -145,   104,  -145,   111,    99,   112,
     102,   113,   121,   115,   123,   116,   124,   122,  -145,  -145,
      49,   -16,   -16,  -145,   -39,    30,  -145,    28,    37,  -145,
      51,    91,  -145,   157,    38,    98,  -145,    91,   -16,    98,
     -16,    98,   -16,    98,   -16,    98,   -16,    98,   -16,  -145,
    -145,  -145,  -145,  -145,   -39,   120,    70,  -145,   -39,  -145,
    -145,    49,  -145,    98,  -145,   129,  -145,  -145,  -145,    98,
    -145,  -145,  -145,  -145,  -145,    57,  -145,    -1,  -145,  -145,
    -145,  -145,  -145,  -145,   160,  -145,  -145,  -145,   130,  -145,
     131,  -145,   132,  -145,   134,  -145,   135,  -145,  -145,   149,
    -145,   142,   120,  -145,   139,  -145,  -145,    91,  -145,  -145,
    -145,  -145,   -16,   -16,   -16,   -16,   -16,   181,   182,    -1,
     186,   141,  -145,  -145,  -145,  -145,  -145,  -145,    -1,    -1,
     186,   157,   185,  -145,  -145,  -145,  -145,   143,  -145,   172,
    -145,    -1,    -1,    -1,     0,  -145,  -145,  -145,  -145,  -145,
    -145
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       4,     3,    11,    12,    13,    14,     0,     5,     0,     0,
       9,     6,    10,     7,     8,    15,     0,     0,     0,     0,
       0,   108,    20,     0,     0,     0,   106,   107,     0,     0,
       0,     0,     0,     0,   109,    76,    63,    77,     0,     0,
      47,     0,     1,     2,     0,     0,     0,    19,     0,     0,
      43,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    49,    51,     0,    16,     0,     0,     0,     0,
       0,    24,   109,    43,    73,     0,    17,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    64,    79,
      96,    47,    47,    48,     0,     0,    28,     0,     0,    30,
       0,     0,    45,    44,     0,     0,    25,     0,    47,     0,
      47,     0,    47,     0,    47,     0,    47,     0,    47,   101,
     102,   103,   104,   105,     0,    43,    97,    98,     0,    78,
      50,    96,    18,     0,    33,     0,    35,    32,    21,     0,
      22,    40,    38,    39,    41,     0,    36,     0,    69,    68,
      70,    65,    66,    67,     0,    74,    75,    52,     0,    53,
       0,    55,     0,    57,     0,    59,     0,    61,    80,    88,
      99,     0,    43,    29,     0,    31,    23,     0,    46,    71,
      72,    42,    47,    47,    47,    47,    47,     0,    92,     0,
      82,     0,    37,    54,    56,    58,    60,    62,     0,     0,
      82,   100,     0,    27,    34,    89,    90,    87,    94,    93,
      26,     0,     0,     0,    86,    81,    91,    95,    85,    84,
      83
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -145,  -145,  -145,  -145,  -145,  -145,  -145,  -145,  -145,   127,
      65,  -145,  -145,  -112,  -144,   -80,    10,   -85,    -9,  -145,
    -145,  -145,  -145,    92,   159,  -128,   107,     9,  -145,  -145,
    -145,     2,  -145,  -145,  -145,    79,  -145,    85,  -145,  -145,
      -3,     7
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,    18,    19,    20,    21,    22,    23,    24,   105,   108,
     106,   147,   155,   156,   112,    81,   113,    73,   114,    47,
     164,   191,    83,    84,    48,    99,   100,   213,   225,   230,
     198,   216,   217,   210,   219,   135,   136,   137,   138,    38,
      49,    50
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      46,    32,    25,   116,    35,   166,   178,    27,   228,    29,
     181,    80,    44,   188,   229,    31,   139,   140,    39,    40,
      41,    42,    43,    87,    55,    56,    57,    58,    71,    26,
      59,    60,    28,   167,    30,   169,    34,   171,    72,   173,
      51,   175,    46,   177,    33,    39,    40,    41,    42,    43,
     115,    75,   189,    44,    52,   179,   144,   145,   146,    98,
      88,    90,    92,    94,    96,   218,   101,    53,    85,    54,
      89,    91,    93,    95,    97,   202,    61,   102,    62,   227,
      44,    63,   129,   107,   109,   109,    36,    37,   158,   159,
     160,    45,   142,   143,   130,   131,   132,   133,   161,   148,
     149,   101,   200,   129,   162,   163,  -108,   203,   204,   205,
     206,   207,   134,   150,   149,   130,   131,   132,   133,   186,
     187,    64,    85,    65,    66,    67,   168,    69,   170,    68,
     172,   101,   174,    70,   176,   101,     1,    76,     2,    77,
       3,     4,     5,    78,    80,     6,   151,   152,   153,   154,
     107,    79,    82,    86,    44,   190,   185,   103,     7,     8,
       9,   104,   111,   119,   117,   197,   121,    10,    11,    12,
      13,    14,    15,   118,   120,   122,    16,   124,   126,    39,
      40,    41,    42,    43,   128,   123,    17,   125,   127,   157,
     184,   199,   192,   193,   194,   201,   195,   196,   208,   215,
     209,   212,   221,   214,   223,   110,   222,   165,   183,   211,
      74,   141,   224,   215,    44,   151,   152,   153,   154,   220,
     182,   180,     0,     0,   226
};

static const yytype_int16 yycheck[] =
{
       9,     4,     4,    83,     7,   117,   134,     6,     8,     6,
     138,    24,    54,   157,    14,    54,   101,   102,    19,    20,
      21,    22,    23,    65,    27,    28,    29,    30,    44,    31,
      33,    34,    31,   118,    31,   120,    13,   122,    54,   124,
      27,   126,    51,   128,    10,    19,    20,    21,    22,    23,
      63,    54,   164,    54,     0,   135,    28,    29,    30,    68,
      63,    64,    65,    66,    67,   209,    69,    59,    61,    13,
      63,    64,    65,    66,    67,   187,    26,    70,    60,   223,
      54,    61,    33,    76,    77,    78,    41,    42,    50,    51,
      52,    65,    62,    63,    45,    46,    47,    48,    60,    62,
      63,   104,   182,    33,    66,    67,    64,   192,   193,   194,
     195,   196,    63,    62,    63,    45,    46,    47,    48,    62,
      63,    61,   115,    61,    61,    61,   119,    13,   121,    63,
     123,   134,   125,    64,   127,   138,     3,    61,     5,    61,
       7,     8,     9,    61,    24,    12,    55,    56,    57,    58,
     143,    11,    54,    58,    54,   164,   149,    54,    25,    26,
      27,    13,    61,    64,    60,    16,    64,    34,    35,    36,
      37,    38,    39,    62,    62,    62,    43,    62,    62,    19,
      20,    21,    22,    23,    62,    64,    53,    64,    64,    32,
      61,    49,    62,    62,    62,    56,    62,    62,    17,   208,
      18,    15,    17,    62,    32,    78,    63,   115,   143,   199,
      51,   104,   221,   222,    54,    55,    56,    57,    58,   210,
     141,   136,    -1,    -1,   222
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     3,     5,     7,     8,     9,    12,    25,    26,    27,
      34,    35,    36,    37,    38,    39,    43,    53,    69,    70,
      71,    72,    73,    74,    75,     4,    31,     6,    31,     6,
      31,    54,   108,    10,    13,   108,    41,    42,   107,    19,
      20,    21,    22,    23,    54,    65,    86,    87,    92,   108,
     109,    27,     0,    59,    13,   108,   108,   108,   108,   108,
     108,    26,    60,    61,    61,    61,    61,    61,    63,    13,
      64,    44,    54,    85,    92,   108,    61,    61,    61,    11,
      24,    83,    54,    90,    91,   109,    58,    65,   108,   109,
     108,   109,   108,   109,   108,   109,   108,   109,    86,    93,
      94,   108,   109,    54,    13,    76,    78,   109,    77,   109,
      77,    61,    82,    84,    86,    63,    83,    60,    62,    64,
      62,    64,    62,    64,    62,    64,    62,    64,    62,    33,
      45,    46,    47,    48,    63,   103,   104,   105,   106,    85,
      85,    94,    62,    63,    28,    29,    30,    79,    62,    63,
      62,    55,    56,    57,    58,    80,    81,    32,    50,    51,
      52,    60,    66,    67,    88,    91,    81,    85,   109,    85,
     109,    85,   109,    85,   109,    85,   109,    85,    93,    83,
     105,    93,   103,    78,    61,   109,    62,    63,    82,    81,
      86,    89,    62,    62,    62,    62,    62,    16,    98,    49,
      83,    56,    81,    85,    85,    85,    85,    85,    17,    18,
     101,    84,    15,    95,    62,    86,    99,   100,    82,   102,
      95,    17,    63,    32,    86,    96,    99,    82,     8,    14,
      97
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    68,    69,    69,    69,    69,    70,    70,    70,    70,
      70,    71,    71,    71,    71,    72,    72,    73,    74,    74,
      74,    74,    74,    75,    75,    75,    75,    75,    76,    76,
      77,    77,    78,    79,    79,    79,    80,    80,    81,    81,
      81,    81,    82,    83,    83,    84,    84,    85,    85,    85,
      86,    86,    86,    86,    86,    86,    86,    86,    86,    86,
      86,    86,    86,    87,    87,    88,    88,    88,    88,    88,
      88,    89,    89,    90,    90,    91,    92,    92,    93,    94,
      94,    95,    95,    96,    97,    97,    97,    98,    98,    99,
     100,   100,   101,   101,   102,   102,   103,   103,   104,   104,
     105,   106,   106,   106,   106,   106,   107,   107,   108,   109
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     2,     4,     4,     6,     3,
       2,     6,     6,     7,     4,     5,     9,     8,     1,     3,
       1,     3,     2,     1,     4,     1,     1,     3,     1,     1,
       1,     1,     3,     0,     2,     1,     3,     0,     2,     1,
       4,     2,     5,     5,     7,     5,     7,     5,     7,     5,
       7,     5,     7,     1,     3,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     3,     3,     1,     1,     2,     1,
       3,     3,     0,     2,     1,     1,     0,     3,     0,     1,
       1,     3,     0,     2,     1,     3,     0,     1,     1,     2,
       4,     1,     1,     1,     1,     1,     1,     1,     1,     1
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (&yylloc, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF

/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)                                \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;        \
          (Current).first_column = YYRHSLOC (Rhs, 1).first_column;      \
          (Current).last_line    = YYRHSLOC (Rhs, N).last_line;         \
          (Current).last_column  = YYRHSLOC (Rhs, N).last_column;       \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).first_line   = (Current).last_line   =              \
            YYRHSLOC (Rhs, 0).last_line;                                \
          (Current).first_column = (Current).last_column =              \
            YYRHSLOC (Rhs, 0).last_column;                              \
        }                                                               \
    while (0)
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K])


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)


/* YYLOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

# ifndef YYLOCATION_PRINT

#  if defined YY_LOCATION_PRINT

   /* Temporary convenience wrapper in case some people defined the
      undocumented and private YY_LOCATION_PRINT macros.  */
#   define YYLOCATION_PRINT(File, Loc)  YY_LOCATION_PRINT(File, *(Loc))

#  elif defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL

/* Print *YYLOCP on YYO.  Private, do not rely on its existence. */

YY_ATTRIBUTE_UNUSED
static int
yy_location_print_ (FILE *yyo, YYLTYPE const * const yylocp)
{
  int res = 0;
  int end_col = 0 != yylocp->last_column ? yylocp->last_column - 1 : 0;
  if (0 <= yylocp->first_line)
    {
      res += YYFPRINTF (yyo, "%d", yylocp->first_line);
      if (0 <= yylocp->first_column)
        res += YYFPRINTF (yyo, ".%d", yylocp->first_column);
    }
  if (0 <= yylocp->last_line)
    {
      if (yylocp->first_line < yylocp->last_line)
        {
          res += YYFPRINTF (yyo, "-%d", yylocp->last_line);
          if (0 <= end_col)
            res += YYFPRINTF (yyo, ".%d", end_col);
        }
      else if (0 <= end_col && yylocp->first_column < end_col)
        res += YYFPRINTF (yyo, "-%d", end_col);
    }
  return res;
}

#   define YYLOCATION_PRINT  yy_location_print_

    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT(File, Loc)  YYLOCATION_PRINT(File, &(Loc))

#  else

#   define YYLOCATION_PRINT(File, Loc) ((void) 0)
    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT  YYLOCATION_PRINT

#  endif
# endif /* !defined YYLOCATION_PRINT */


# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, Location); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (yylocationp);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  YYLOCATION_PRINT (yyo, yylocationp);
  YYFPRINTF (yyo, ": ");
  yy_symbol_value_print (yyo, yykind, yyvaluep, yylocationp);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp, YYLTYPE *yylsp,
                 int yyrule)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)],
                       &(yylsp[(yyi + 1) - (yynrhs)]));
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, yylsp, Rule); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


/* Context of a parse error.  */
typedef struct
{
  yy_state_t *yyssp;
  yysymbol_kind_t yytoken;
  YYLTYPE *yylloc;
} yypcontext_t;

/* Put in YYARG at most YYARGN of the expected tokens given the
   current YYCTX, and return the number of tokens stored in YYARG.  If
   YYARG is null, return the number of expected tokens (guaranteed to
   be less than YYNTOKENS).  Return YYENOMEM on memory exhaustion.
   Return 0 if there are more than YYARGN expected tokens, yet fill
   YYARG up to YYARGN. */
static int
yypcontext_expected_tokens (const yypcontext_t *yyctx,
                            yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  int yyn = yypact[+*yyctx->yyssp];
  if (!yypact_value_is_default (yyn))
    {
      /* Start YYX at -YYN if negative to avoid negative indexes in
         YYCHECK.  In other words, skip the first -YYN actions for
         this state because they are default actions.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;
      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yyx;
      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
        if (yycheck[yyx + yyn] == yyx && yyx != YYSYMBOL_YYerror
            && !yytable_value_is_error (yytable[yyx + yyn]))
          {
            if (!yyarg)
              ++yycount;
            else if (yycount == yyargn)
              return 0;
            else
              yyarg[yycount++] = YY_CAST (yysymbol_kind_t, yyx);
          }
    }
  if (yyarg && yycount == 0 && 0 < yyargn)
    yyarg[0] = YYSYMBOL_YYEMPTY;
  return yycount;
}




#ifndef yystrlen
# if defined __GLIBC__ && defined _STRING_H
#  define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
# else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
# endif
#endif

#ifndef yystpcpy
# if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#  define yystpcpy stpcpy
# else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
# endif
#endif

#ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYPTRDIFF_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYPTRDIFF_T yyn = 0;
      char const *yyp = yystr;
      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (yyres)
    return yystpcpy (yyres, yystr) - yyres;
  else
    return yystrlen (yystr);
}
#endif


static int
yy_syntax_error_arguments (const yypcontext_t *yyctx,
                           yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yyctx->yytoken != YYSYMBOL_YYEMPTY)
    {
      int yyn;
      if (yyarg)
        yyarg[yycount] = yyctx->yytoken;
      ++yycount;
      yyn = yypcontext_expected_tokens (yyctx,
                                        yyarg ? yyarg + 1 : yyarg, yyargn - 1);
      if (yyn == YYENOMEM)
        return YYENOMEM;
      else
        yycount += yyn;
    }
  return yycount;
}

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return -1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return YYENOMEM if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                const yypcontext_t *yyctx)
{
  enum { YYARGS_MAX = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  yysymbol_kind_t yyarg[YYARGS_MAX];
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* Actual size of YYARG. */
  int yycount = yy_syntax_error_arguments (yyctx, yyarg, YYARGS_MAX);
  if (yycount == YYENOMEM)
    return YYENOMEM;

  switch (yycount)
    {
#define YYCASE_(N, S)                       \
      case N:                               \
        yyformat = S;                       \
        break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
    }

  /* Compute error message size.  Don't count the "%s"s, but reserve
     room for the terminator.  */
  yysize = yystrlen (yyformat) - 2 * yycount + 1;
  {
    int yyi;
    for (yyi = 0; yyi < yycount; ++yyi)
      {
        YYPTRDIFF_T yysize1
          = yysize + yytnamerr (YY_NULLPTR, yytname[yyarg[yyi]]);
        if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
          yysize = yysize1;
        else
          return YYENOMEM;
      }
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return -1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yytname[yyarg[yyi++]]);
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, YYLTYPE *yylocationp)
{
  YY_USE (yyvaluep);
  YY_USE (yylocationp);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}






/*----------.
| yyparse.  |
`----------*/

int
yyparse (void)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

/* Location data for the lookahead symbol.  */
static YYLTYPE yyloc_default
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
  = { 1, 1, 1, 1 }
# endif
;
YYLTYPE yylloc = yyloc_default;

    /* Number of syntax errors so far.  */
    int yynerrs = 0;

    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

    /* The location stack: array, bottom, top.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls = yylsa;
    YYLTYPE *yylsp = yyls;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

  /* The locations where the error started and ended.  */
  YYLTYPE yyerror_range[3];

  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  yylsp[0] = yylloc;
  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;
        YYLTYPE *yyls1 = yyls;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yyls1, yysize * YYSIZEOF (*yylsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
        yyls = yyls1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
        YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval, &yylloc);
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      yyerror_range[1] = yylloc;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END
  *++yylsp = yylloc;

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location. */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  yyerror_range[1] = yyloc;
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* start: stmt ';'  */
#line 100 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        parse_tree = (yyvsp[-1].sv_node);                    // 设置解析结果
        YYACCEPT;                           // 成功完成解析
    }
#line 1742 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 3: /* start: HELP  */
#line 105 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        parse_tree = std::make_shared<Help>();
        YYACCEPT;
    }
#line 1751 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 4: /* start: EXIT  */
#line 110 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        parse_tree = nullptr;               // 空树表示退出
        YYACCEPT;
    }
#line 1760 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 5: /* start: T_EOF  */
#line 115 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
#line 1769 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 11: /* txnStmt: TXN_BEGIN  */
#line 133 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnBegin>();
    }
#line 1777 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 12: /* txnStmt: TXN_COMMIT  */
#line 137 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnCommit>();
    }
#line 1785 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 13: /* txnStmt: TXN_ABORT  */
#line 141 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnAbort>();
    }
#line 1793 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 14: /* txnStmt: TXN_ROLLBACK  */
#line 145 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnRollback>();
    }
#line 1801 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 15: /* dbStmt: SHOW TABLES  */
#line 153 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<ShowTables>();
    }
#line 1809 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 16: /* dbStmt: SHOW INDEX FROM tbName  */
#line 157 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<ShowIndex>((yyvsp[0].sv_str));
    }
#line 1817 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 17: /* setStmt: SET set_knob_type '=' VALUE_BOOL  */
#line 165 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<SetStmt>((yyvsp[-2].sv_setKnobType), (yyvsp[0].sv_bool));
    }
#line 1825 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 18: /* ddl: CREATE TABLE tbName '(' fieldList ')'  */
#line 173 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<CreateTable>((yyvsp[-3].sv_str), (yyvsp[-1].sv_fields));
    }
#line 1833 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 19: /* ddl: DROP TABLE tbName  */
#line 177 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DropTable>((yyvsp[0].sv_str));
    }
#line 1841 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 20: /* ddl: DESC tbName  */
#line 181 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DescTable>((yyvsp[0].sv_str));
    }
#line 1849 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 21: /* ddl: CREATE INDEX tbName '(' colNameList ')'  */
#line 185 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<CreateIndex>((yyvsp[-3].sv_str), (yyvsp[-1].sv_strs));
    }
#line 1857 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 22: /* ddl: DROP INDEX tbName '(' colNameList ')'  */
#line 189 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DropIndex>((yyvsp[-3].sv_str), (yyvsp[-1].sv_strs));
    }
#line 1865 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 23: /* dml: INSERT INTO tbName VALUES '(' valueList ')'  */
#line 197 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<InsertStmt>((yyvsp[-4].sv_str), (yyvsp[-1].sv_vals));
    }
#line 1873 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 24: /* dml: DELETE FROM tbName optWhereClause  */
#line 201 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DeleteStmt>((yyvsp[-1].sv_str), (yyvsp[0].sv_conds));
    }
#line 1881 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 25: /* dml: UPDATE tbName SET setClauses optWhereClause  */
#line 205 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<UpdateStmt>((yyvsp[-3].sv_str), (yyvsp[-1].sv_set_clauses), (yyvsp[0].sv_conds));
    }
#line 1889 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 26: /* dml: SELECT selector FROM tableList optJoinExprs optWhereClause opt_group_clause opt_having_conds opt_order_clause  */
#line 209 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<SelectStmt>((yyvsp[-7].sv_cols), (yyvsp[-5].sv_table_refs), (yyvsp[-4].sv_join_exprs), (yyvsp[-3].sv_conds), (yyvsp[-2].sv_groupbys), (yyvsp[-1].sv_having_conds), (yyvsp[0].sv_orderby));
    }
#line 1897 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 27: /* dml: EXPLAIN SELECT selector FROM tableList optJoinExprs optWhereClause opt_order_clause  */
#line 213 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<ExplainStmt>((yyvsp[-5].sv_cols), (yyvsp[-3].sv_table_refs), (yyvsp[-2].sv_join_exprs), (yyvsp[-1].sv_conds), (yyvsp[0].sv_orderby));
    }
#line 1905 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 28: /* fieldList: field  */
#line 220 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_fields) = std::vector<std::shared_ptr<Field>>{(yyvsp[0].sv_field)};
    }
#line 1913 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 29: /* fieldList: fieldList ',' field  */
#line 224 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_fields).push_back((yyvsp[0].sv_field));                   // 向现有列表添加新字段
    }
#line 1921 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 30: /* colNameList: colName  */
#line 232 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_strs) = std::vector<std::string>{(yyvsp[0].sv_str)};
    }
#line 1929 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 31: /* colNameList: colNameList ',' colName  */
#line 236 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_strs).push_back((yyvsp[0].sv_str));                   // 向现有列表添加新列名
    }
#line 1937 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 32: /* field: colName type  */
#line 244 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_field) = std::make_shared<ColDef>((yyvsp[-1].sv_str), (yyvsp[0].sv_type_len));
    }
#line 1945 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 33: /* type: INT  */
#line 252 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = std::make_shared<TypeLen>(SV_TYPE_INT, sizeof(int));
    }
#line 1953 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 34: /* type: CHAR '(' VALUE_INT ')'  */
#line 256 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = std::make_shared<TypeLen>(SV_TYPE_STRING, (yyvsp[-1].sv_int));
    }
#line 1961 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 35: /* type: FLOAT  */
#line 260 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = std::make_shared<TypeLen>(SV_TYPE_FLOAT, sizeof(float));
    }
#line 1969 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 36: /* valueList: value  */
#line 268 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_vals) = std::vector<std::shared_ptr<Value>>{(yyvsp[0].sv_val)};
    }
#line 1977 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 37: /* valueList: valueList ',' value  */
#line 272 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_vals).push_back((yyvsp[0].sv_val));                   // 向现有列表添加新值
    }
#line 1985 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 38: /* value: VALUE_INT  */
#line 280 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<IntLit>((yyvsp[0].sv_int));
    }
#line 1993 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 39: /* value: VALUE_FLOAT  */
#line 284 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<FloatLit>((yyvsp[0].sv_float));
    }
#line 2001 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 40: /* value: VALUE_STRING  */
#line 288 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<StringLit>((yyvsp[0].sv_str));
    }
#line 2009 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 41: /* value: VALUE_BOOL  */
#line 292 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<BoolLit>((yyvsp[0].sv_bool));
    }
#line 2017 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 42: /* condition: col op expr  */
#line 300 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_cond) = std::make_shared<BinaryExpr>((yyvsp[-2].sv_col), (yyvsp[-1].sv_comp_op), (yyvsp[0].sv_expr));
    }
#line 2025 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 43: /* optWhereClause: %empty  */
#line 308 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    { 
        /* 不做任何操作，保持默认值 */ 
    }
#line 2033 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 44: /* optWhereClause: WHERE whereClause  */
#line 312 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_conds) = (yyvsp[0].sv_conds);                            // 传递WHERE子句的内容
    }
#line 2041 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 45: /* whereClause: condition  */
#line 320 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_conds) = std::vector<std::shared_ptr<BinaryExpr>>{(yyvsp[0].sv_cond)};
    }
#line 2049 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 46: /* whereClause: whereClause AND condition  */
#line 324 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_conds).push_back((yyvsp[0].sv_cond));                   // 向条件列表添加新条件
    }
#line 2057 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 47: /* optAlias: %empty  */
#line 332 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_str) = "";                            // 空字符串表示没有别名
    }
#line 2065 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 48: /* optAlias: AS IDENTIFIER  */
#line 336 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_str) = (yyvsp[0].sv_str);                            // 返回别名
    }
#line 2073 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 49: /* optAlias: IDENTIFIER  */
#line 340 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_str) = (yyvsp[0].sv_str);                            // 返回别名
    }
#line 2081 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 50: /* col: tbName '.' colName optAlias  */
#line 348 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-3].sv_str), (yyvsp[-1].sv_str), (yyvsp[0].sv_str));
    }
#line 2089 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 51: /* col: colName optAlias  */
#line 352 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>("", (yyvsp[-1].sv_str), (yyvsp[0].sv_str)); // 表名为空字符串
    }
#line 2097 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 52: /* col: COUNT '(' '*' ')' optAlias  */
#line 356 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>("", "*", (yyvsp[0].sv_str), SV_AGGREGATE_COUNT); // 特殊处理COUNT(*)
    }
#line 2105 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 53: /* col: COUNT '(' colName ')' optAlias  */
#line 360 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>("", (yyvsp[-2].sv_str), (yyvsp[0].sv_str), SV_AGGREGATE_COUNT);
    }
#line 2113 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 54: /* col: COUNT '(' tbName '.' colName ')' optAlias  */
#line 364 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-4].sv_str), (yyvsp[-2].sv_str), (yyvsp[0].sv_str), SV_AGGREGATE_COUNT);
    }
#line 2121 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 55: /* col: SUM '(' colName ')' optAlias  */
#line 368 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>("", (yyvsp[-2].sv_str), (yyvsp[0].sv_str), SV_AGGREGATE_SUM);
    }
#line 2129 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 56: /* col: SUM '(' tbName '.' colName ')' optAlias  */
#line 372 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-4].sv_str), (yyvsp[-2].sv_str), (yyvsp[0].sv_str), SV_AGGREGATE_SUM);
    }
#line 2137 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 57: /* col: AVG '(' colName ')' optAlias  */
#line 376 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>("", (yyvsp[-2].sv_str), (yyvsp[0].sv_str), SV_AGGREGATE_AVG);
    }
#line 2145 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 58: /* col: AVG '(' tbName '.' colName ')' optAlias  */
#line 380 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-4].sv_str), (yyvsp[-2].sv_str), (yyvsp[0].sv_str), SV_AGGREGATE_AVG);
    }
#line 2153 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 59: /* col: MIN '(' colName ')' optAlias  */
#line 384 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>("", (yyvsp[-2].sv_str), (yyvsp[0].sv_str), SV_AGGREGATE_MIN);
    }
#line 2161 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 60: /* col: MIN '(' tbName '.' colName ')' optAlias  */
#line 388 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-4].sv_str), (yyvsp[-2].sv_str), (yyvsp[0].sv_str), SV_AGGREGATE_MIN);
    }
#line 2169 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 61: /* col: MAX '(' colName ')' optAlias  */
#line 392 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>("", (yyvsp[-2].sv_str), (yyvsp[0].sv_str), SV_AGGREGATE_MAX);
    }
#line 2177 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 62: /* col: MAX '(' tbName '.' colName ')' optAlias  */
#line 396 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-4].sv_str), (yyvsp[-2].sv_str), (yyvsp[0].sv_str), SV_AGGREGATE_MAX);
    }
#line 2185 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 63: /* colList: col  */
#line 404 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_cols) = std::vector<std::shared_ptr<Col>>{(yyvsp[0].sv_col)};
    }
#line 2193 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 64: /* colList: colList ',' col  */
#line 408 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_cols).push_back((yyvsp[0].sv_col));                   // 向列表添加新列
    }
#line 2201 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 65: /* op: '='  */
#line 416 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_EQ;
    }
#line 2209 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 66: /* op: '<'  */
#line 420 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_LT;
    }
#line 2217 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 67: /* op: '>'  */
#line 424 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_GT;
    }
#line 2225 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 68: /* op: NEQ  */
#line 428 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_NE;
    }
#line 2233 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 69: /* op: LEQ  */
#line 432 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_LE;
    }
#line 2241 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 70: /* op: GEQ  */
#line 436 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_GE;
    }
#line 2249 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 71: /* expr: value  */
#line 444 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_expr) = std::static_pointer_cast<Expr>((yyvsp[0].sv_val));
    }
#line 2257 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 72: /* expr: col  */
#line 448 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_expr) = std::static_pointer_cast<Expr>((yyvsp[0].sv_col));
    }
#line 2265 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 73: /* setClauses: setClause  */
#line 456 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clauses) = std::vector<std::shared_ptr<SetClause>>{(yyvsp[0].sv_set_clause)};
    }
#line 2273 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 74: /* setClauses: setClauses ',' setClause  */
#line 460 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clauses).push_back((yyvsp[0].sv_set_clause));                   // 向列表添加新的SET子句
    }
#line 2281 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 75: /* setClause: colName '=' value  */
#line 468 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = std::make_shared<SetClause>((yyvsp[-2].sv_str), (yyvsp[0].sv_val));
    }
#line 2289 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 76: /* selector: '*'  */
#line 476 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_cols) = {};                            // 空向量表示选择所有列
    }
#line 2297 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 78: /* tableRef: tbName optAlias  */
#line 485 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        
            (yyval.sv_table_ref) = std::make_shared<TableRef>((yyvsp[-1].sv_str), (yyvsp[0].sv_str));
        
    }
#line 2307 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 79: /* tableList: tableRef  */
#line 495 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_table_refs) = std::vector<std::shared_ptr<TableRef>>{(yyvsp[0].sv_table_ref)};
    }
#line 2315 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 80: /* tableList: tableList ',' tableRef  */
#line 499 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_table_refs).push_back((yyvsp[0].sv_table_ref));                   // 向表列表添加新表
    }
#line 2323 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 81: /* opt_order_clause: ORDER BY order_clause  */
#line 507 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    { 
        (yyval.sv_orderby) = (yyvsp[0].sv_orderby); 
    }
#line 2331 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 82: /* opt_order_clause: %empty  */
#line 511 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    { 
        /* 不做任何操作，保持默认值 */ 
    }
#line 2339 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 83: /* order_clause: col opt_asc_desc  */
#line 519 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    { 
        (yyval.sv_orderby) = std::make_shared<OrderBy>((yyvsp[-1].sv_col), (yyvsp[0].sv_orderby_dir));
    }
#line 2347 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 84: /* opt_asc_desc: ASC  */
#line 527 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    { 
        (yyval.sv_orderby_dir) = OrderBy_ASC; 
    }
#line 2355 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 85: /* opt_asc_desc: DESC  */
#line 531 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    { 
        (yyval.sv_orderby_dir) = OrderBy_DESC; 
    }
#line 2363 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 86: /* opt_asc_desc: %empty  */
#line 535 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    { 
        (yyval.sv_orderby_dir) = OrderBy_DEFAULT; 
    }
#line 2371 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 87: /* opt_group_clause: GROUP BY group_clauses  */
#line 542 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_groupbys) = (yyvsp[0].sv_groupbys);
    }
#line 2379 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 88: /* opt_group_clause: %empty  */
#line 545 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
                      { /* ignore*/ }
#line 2385 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 89: /* group_clause: col  */
#line 549 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_groupby) = std::make_shared<GroupBy>((yyvsp[0].sv_col));
    }
#line 2393 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 90: /* group_clauses: group_clause  */
#line 554 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_groupbys) = std::vector<std::shared_ptr<GroupBy>>{(yyvsp[0].sv_groupby)};
    }
#line 2401 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 91: /* group_clauses: group_clauses ',' group_clause  */
#line 558 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_groupbys).push_back((yyvsp[0].sv_groupby));
    }
#line 2409 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 92: /* opt_having_conds: %empty  */
#line 563 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
                     { /* ignore*/ }
#line 2415 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 93: /* opt_having_conds: HAVING having_conds  */
#line 565 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_having_conds) = (yyvsp[0].sv_having_conds);
    }
#line 2423 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 94: /* having_conds: condition  */
#line 571 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_having_conds) = std::vector<std::shared_ptr<BinaryExpr>>{(yyvsp[0].sv_cond)};
    }
#line 2431 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 95: /* having_conds: having_conds AND condition  */
#line 575 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_having_conds).push_back((yyvsp[0].sv_cond));
    }
#line 2439 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 96: /* optJoinExprs: %empty  */
#line 584 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_join_exprs) = std::vector<std::shared_ptr<JoinExpr>>{};
    }
#line 2447 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 97: /* optJoinExprs: joinExprs  */
#line 588 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_join_exprs) = (yyvsp[0].sv_join_exprs);
    }
#line 2455 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 98: /* joinExprs: joinExpr  */
#line 596 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_join_exprs) = std::vector<std::shared_ptr<JoinExpr>>{(yyvsp[0].sv_join_expr)};
    }
#line 2463 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 99: /* joinExprs: joinExprs joinExpr  */
#line 600 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_join_exprs).push_back((yyvsp[0].sv_join_expr));
    }
#line 2471 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 100: /* joinExpr: joinType tableRef ON whereClause  */
#line 608 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_join_expr) = std::make_shared<JoinExpr>((yyvsp[-2].sv_table_ref), (yyvsp[0].sv_conds), (yyvsp[-3].sv_join_type));
    }
#line 2479 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 101: /* joinType: JOIN  */
#line 616 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_join_type) = SV_INNER_JOIN;
    }
#line 2487 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 102: /* joinType: INNER_JOIN  */
#line 620 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_join_type) = SV_INNER_JOIN;
    }
#line 2495 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 103: /* joinType: LEFT_JOIN  */
#line 624 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_join_type) = SV_LEFT_JOIN;
    }
#line 2503 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 104: /* joinType: RIGHT_JOIN  */
#line 628 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_join_type) = SV_RIGHT_JOIN;
    }
#line 2511 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 105: /* joinType: FULL_JOIN  */
#line 632 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_join_type) = SV_FULL_JOIN;
    }
#line 2519 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 106: /* set_knob_type: ENABLE_NESTLOOP  */
#line 640 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_setKnobType) = EnableNestLoop;
    }
#line 2527 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;

  case 107: /* set_knob_type: ENABLE_SORTMERGE  */
#line 644 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"
    {
        (yyval.sv_setKnobType) = EnableSortMerge;
    }
#line 2535 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"
    break;


#line 2539 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.tab.cpp"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      {
        yypcontext_t yyctx
          = {yyssp, yytoken, &yylloc};
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == -1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *,
                             YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (yymsg)
              {
                yysyntax_error_status
                  = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
                yymsgp = yymsg;
              }
            else
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = YYENOMEM;
              }
          }
        yyerror (&yylloc, yymsgp);
        if (yysyntax_error_status == YYENOMEM)
          YYNOMEM;
      }
    }

  yyerror_range[1] = yylloc;
  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, &yylloc);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, yylsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  yyerror_range[2] = yylloc;
  ++yylsp;
  YYLLOC_DEFAULT (*yylsp, yyerror_range, 2);

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, yylsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
  return yyresult;
}

#line 653 "/home/fuyuki_vila/Desktop/DB2025/src/parser/yacc.y"


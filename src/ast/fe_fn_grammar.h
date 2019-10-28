/*
 * fe_fn_grammer.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AST_FE_FN_GRAMMAR_H_
#define AST_FE_FN_GRAMMAR_H_

#include "tao/pegtl.hpp"

namespace fesql {
namespace ast {

// integer 32
struct i32 : tao::pegtl::seq< tao::pegtl::opt< tao::pegtl::one< '+', '-' > >,
                              tao::pegtl::plus< tao::pegtl::digit > > {};

struct f32 : tao::pegtl::seq< tao::pegtl::plus< tao::pegtl::digit > , tao::pegtl::one<'.'> ,
             tao::pegtl::star<tao::pegtl::digit> > {};
struct s1 : tao::pegtl::plus< tao::pegtl::blank > {};
struct s0 : tao::pegtl::star< tao::pegtl::blank > {};

struct assign : tao::pegtl::one< '=' > {};
struct co : tao::pegtl::one< ':' > {};
struct open_bracket : tao::pegtl::seq<tao::pegtl::one< '(' >, s0> {};
struct close_bracket : tao::pegtl::seq<s0, tao::pegtl::one< ')' > > {};

struct str_i32: TAO_PEGTL_STRING("i32") {};
struct str_i64: TAO_PEGTL_STRING("i64") {};
struct str_f32: TAO_PEGTL_STRING("f32") {};
struct str_f64: TAO_PEGTL_STRING("f64") {};
struct str_str: TAO_PEGTL_STRING("str") {};
struct str_date: TAO_PEGTL_STRING("date") {};
struct str_timestamp: TAO_PEGTL_STRING("timestamp") {};

struct type: tao::pegtl::sor<str_i32, str_i64, str_f64, str_f32, str_str, str_date, str_timestamp> {};
struct primary: tao::pegtl::sor<i32, f32> {};
struct plus : tao::pegtl::pad< tao::pegtl::one< '+' >, tao::pegtl::space > {};
struct minus : tao::pegtl::pad< tao::pegtl::one< '-' >, tao::pegtl::space > {};
struct multiply : tao::pegtl::pad< tao::pegtl::one< '*' >, tao::pegtl::space > {};
struct divide : tao::pegtl::pad< tao::pegtl::one< '/' >, tao::pegtl::space > {};

struct str_return : TAO_PEGTL_STRING( "return" ) {};
struct str_def : TAO_PEGTL_STRING("def") {};

struct comment: tao::pegtl::if_must< tao::pegtl::one< '#' >, ::tao::pegtl::until< ::tao::pegtl::eolf >>{};
struct expr ;
struct bracketd : tao::pegtl::if_must<open_bracket, expr, close_bracket> {};
struct value: tao::pegtl::sor<primary, tao::pegtl::identifier, bracketd> {};
struct product: tao::pegtl::list_must<value, tao::pegtl::sor<multiply, divide>>  {};
struct expr : tao::pegtl::list_must<product, tao::pegtl::sor<plus, minus>> {};

struct parameter : tao::pegtl::must<tao::pegtl::identifier, 
    tao::pegtl::one<':'>, 
    type > {};

struct fn_def : tao::pegtl::if_must<
                str_def, 
                s1, 
                tao::pegtl::identifier, 
                open_bracket,
                tao::pegtl::list< tao::pegtl::seq<parameter, s0 >, tao::pegtl::one<','> >,
                close_bracket,
                co, tao::pegtl::sor<str_i32, str_i64, str_f32, str_str, str_date, str_timestamp>, tao::pegtl::eolf> {};

struct indent : tao::pegtl::star< tao::pegtl::blank > {};

struct assign_stmt : tao::pegtl::seq<indent, 
    tao::pegtl::seq<tao::pegtl::identifier, assign, expr, tao::pegtl::eolf> > {};

struct return_stmt : tao::pegtl::seq<indent, 
    tao::pegtl::seq<str_return, s0, expr,
    tao::pegtl::eolf> > {};

struct indented : tao::pegtl::must< indent, tao::pegtl::sor< fn_def, assign_stmt, return_stmt> > {};
struct line : tao::pegtl::sor< tao::pegtl::eolf, indented, comment > {};
struct grammar : tao::pegtl::until< tao::pegtl::eof, tao::pegtl::must< line > > {};

template< typename Rule > struct fe_selector : std::true_type {};
template<> struct fe_selector< s0 > : std::false_type {};
template<> struct fe_selector< str_def > : std::false_type {};
template<> struct fe_selector< tao::pegtl::one<','> > : std::false_type {};

} // namespace of ast
} // namespace of fesql
#endif /* !AST_FE_FN_GRAMMAR_H_ */

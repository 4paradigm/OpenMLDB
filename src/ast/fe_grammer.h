/*
 * fe_grammer.h
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

#ifndef AST_FE_GRAMMER_H_
#define AST_FE_GRAMMER_H_

#include "tao/pegtl.hpp"
#include "tao/pegtl/analyze.hpp"

namespace fesql {
namespace ast {

// integer 32
struct i32 : tao::pegtl::seq< tao::pegtl::opt< tao::pegtl::one< '+', '-' > >,
                         tao::pegtl::plus< tao::pegtl::digit > > {};

struct co : tao::pegtl::one< ':' > {};

struct str_i32: TAO_PEGTL_STRING("i32") {};
struct str_return : TAO_PEGTL_STRING( "return" ) {};
struct str_def : TAO_PEGTL_STRING("def") {};

struct s1 : tao::pegtl::plus< tao::pegtl::one< ' ' > > {};
struct parameter : tao::pegtl::must< tao::pegtl::identifier, tao::pegtl::one<':'>, str_i32> {};
struct fn_def : tao::pegtl::if_must<
                str_def, 
                s1, 
                tao::pegtl::identifier, 
                tao::pegtl::one< '('>,
                parameter,
                //tao::pegtl::pad_opt< tao::pegtl::one< ',' >,  parameter>,
                tao::pegtl::one< ')'>,
                co> {};
struct grammer : tao::pegtl::must< fn_def, tao::pegtl::eof > {};

} // namespace of ast
} // namespace of fesql
#endif /* !AST_FE_GRAMMER_H_ */

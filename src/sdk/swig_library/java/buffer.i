/*
 * src/sdk/swig_library/java/buffer.i
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifdef SWIGJAVA

%define %as_direct_buffer(TYPE)

%typemap(jni) TYPE "jobject"
%typemap(jni) const TYPE& "jobject"
%typemap(jtype) TYPE "java.nio.ByteBuffer"
%typemap(jtype) const TYPE& "java.nio.ByteBuffer"
%typemap(jstype) TYPE "java.nio.ByteBuffer"
%typemap(jstype) const TYPE& "java.nio.ByteBuffer"

%typemap(in) TYPE %{
    char* $1_addr = (char *) jenv->GetDirectBufferAddress($input);
   	size_t $1_size = jenv->GetDirectBufferCapacity($input);
  	if ($1_addr == NULL) {  
    	SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException,
    		"Unable to get address of a java.nio.ByteBuffer direct byte buffer. Buffer must be a direct buffer and not a non-direct buffer.");  
  	}
  	$1 = TYPE($1_addr, $1_size);
%}
%typemap(in) const TYPE& %{
    char* $1_addr = (char *) jenv->GetDirectBufferAddress($input);
   	size_t $1_size = jenv->GetDirectBufferCapacity($input);
  	if ($1_addr == NULL) {  
    	SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException,
    		"Unable to get address of a java.nio.ByteBuffer direct byte buffer. Buffer must be a direct buffer and not a non-direct buffer.");  
  	}
  	TYPE $1_inst($1_addr, $1_size);
  	$1 = &($1_inst);
%}

%typemap(javain) TYPE "$javainput"
%typemap(javain) const TYPE & "$javainput"


%enddef


#endif  // 	SWIGJAVA

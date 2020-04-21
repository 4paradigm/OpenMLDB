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
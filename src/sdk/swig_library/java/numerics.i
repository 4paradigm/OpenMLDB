%typemap(jni) int32_t "jint"
%typemap(jtype) int32_t "int"
%typemap(jstype) int32_t "int"
%typemap(javain) int32_t "$javainput"
%typemap(javaout) int32_t "{ return $jnicall; }"
%typemap(in) int32_t %{ $1 = $input; %}
%typemap(out) int32_t %{ $result = $1; %}


%typemap(jni) uint32_t "jint"
%typemap(jtype) uint32_t "int"
%typemap(jstype) uint32_t "int"
%typemap(javain) uint32_t "$javainput"
%typemap(javaout) uint32_t "{ return $jnicall; }"
%typemap(in) uint32_t %{ $1 = $input; %}
%typemap(out) uint32_t %{ $result = $1; %}

%typemap(jni) int64_t "jlong"
%typemap(jtype) int64_t "long"
%typemap(jstype) int64_t "long"
%typemap(javain) int64_t "$javainput"
%typemap(javaout) int64_t "{ return $jnicall; }"
%typemap(in) int32_t %{ $1 = $input; %}
%typemap(in) int64_t %{ $1 = $input; %}
%typemap(out) int64_t %{ $result = $1; %}

%typemap(jni) uint64_t "jlong"
%typemap(jtype) uint64_t "long"
%typemap(jstype) uint64_t "long"
%typemap(javain) uint64_t "$javainput"
%typemap(javaout) uint64_t "{ return $jnicall; }"
%typemap(in) uint64_t %{ $1 = $input; %}
%typemap(out) uint64_t %{ $result = $1; %}
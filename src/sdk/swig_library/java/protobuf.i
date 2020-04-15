#ifdef SWIGJAVA

// Declare auto type mapping for protobuf enums 
%define %protobuf_enum(TYPE, JTYPE)

%typemap(jni) TYPE "jint"
%typemap(jtype) TYPE "int"
%typemap(jstype) TYPE "JTYPE"

%typemap(in) TYPE %{ 
   $1 = (TYPE) ($input);
%}

%typemap(javain) TYPE "($javainput).getNumber()"

%typemap(out) TYPE %{ 
   $result = $1;
%}

%typemap(javaout) TYPE { 
    return $typemap(jstype, TYPE).valueOf($jnicall);
}

%enddef


// Declare auto type mapping for protobuf record 
%define %protobuf(TYPE, JTYPE)

%typemap(jni) TYPE "jbyteArray"
%typemap(jni) const TYPE& "jbyteArray"

%typemap(jtype) TYPE "byte[]"
%typemap(jtype) const TYPE& "byte[]"

%typemap(jstype) TYPE "JTYPE"
%typemap(jstype) const TYPE& "JTYPE"


%typemap(in) TYPE %{
    if(!$input) {
        if (!jenv->ExceptionCheck()) {
            SWIG_JavaThrowException(jenv, SWIG_JavaNullPointerException, "null string");
        }
        return $null;
    }
    jsize $1_bytes_size = jenv->GetArrayLength($input);
    jbyte* $1_bytes = (jbyte *)jenv->GetByteArrayElements($input, NULL);
    ($1).ParseFromArray($1_bytes, $1_bytes_size);
    jenv->ReleaseByteArrayElements($input, $1_bytes, 0);
%}
%typemap(in) const TYPE& %{
    if(!$input) {
        if (!jenv->ExceptionCheck()) {
            SWIG_JavaThrowException(jenv, SWIG_JavaNullPointerException, "null string");
        }
        return $null;
    }
    jsize $1_bytes_size = jenv->GetArrayLength($input);
    jbyte* $1_bytes = (jbyte *)jenv->GetByteArrayElements($input, NULL);
    TYPE $1_object;
    ($1_object).ParseFromArray($1_bytes, $1_bytes_size);
    $1 = &($1_object);
    jenv->ReleaseByteArrayElements($input, $1_bytes, 0);
%}


%typemap(javain) TYPE "($javainput).toByteArray()"
%typemap(javain) const TYPE & "($javainput).toByteArray()"


%typemap(out) TYPE %{
    jsize $1_bytes_size = ($1).ByteSize();
    $result = jenv->NewByteArray($1_bytes_size);
    jbyte* $1_bytes = (jbyte *)jenv->GetByteArrayElements($result, NULL);
    ($1).SerializeToArray($1_bytes, $1_bytes_size);
    jenv->ReleaseByteArrayElements($result, $1_bytes, 0);
%}

%typemap(javaout) TYPE { 
    JTYPE result = null;
    try {
        result = JTYPE.parseFrom($jnicall); 
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        e.printStackTrace();
    }
    return result;
}

%enddef


#endif

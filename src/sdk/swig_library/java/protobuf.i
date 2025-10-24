/*
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


// Declare auto type mapping for protobuf repeated fields
%define %protobuf_repeated_typedef(TYPE, JTYPE)

%typemap(jni) TYPE "jobjectArray"
%typemap(jni) TYPE* "jobjectArray"
%typemap(jni) const TYPE& "jobjectArray"

%typemap(jtype) TYPE "Object[]"
%typemap(jtype) TYPE* "Object[]"
%typemap(jtype) const TYPE& "Object[]"

%typemap(jstype) TYPE "java.util.List<JTYPE>"
%typemap(jstype) TYPE* "java.util.List<JTYPE>"
%typemap(jstype) const TYPE& "java.util.List<JTYPE>"

%typemap(in) TYPE %{
    if(!$input) {
        if (!jenv->ExceptionCheck()) {
            SWIG_JavaThrowException(jenv, SWIG_JavaNullPointerException, "null string");
        }
        return $null;
    }
    jsize $1_fields_size = jenv->GetArrayLength($input);

    jmethodID $1_set_method;

    for (jsize k = 0; k < $1_fields_size; ++k) {
        auto field_ptr = ($1).Add();

        jobject obj = jenv->GetObjectArrayElement($input, k);
        if (k == 0) {
            jclass clazz = jenv->GetObjectClass(obj);
            $1_set_method = jenv->GetMethodID(clazz, "toByteArray", "()[B");
        }

        jbyteArray $1_byte_arr = (jbyteArray) jenv->CallObjectMethod(obj, $1_set_method);
        jsize $1_bytes_size = jenv->GetArrayLength($1_byte_arr);
        jbyte* $1_bytes = (jbyte *)jenv->GetByteArrayElements($1_byte_arr, NULL);
        field_ptr->ParseFromArray($1_bytes, $1_bytes_size);
        jenv->ReleaseByteArrayElements($1_byte_arr, $1_bytes, 0);
    }
%}
%typemap(in) const TYPE& %{
    if(!$input) {
        if (!jenv->ExceptionCheck()) {
            SWIG_JavaThrowException(jenv, SWIG_JavaNullPointerException, "null string");
        }
        return $null;
    }
    jsize $1_fields_size = jenv->GetArrayLength($input);

    jmethodID $1_set_method;

    TYPE $1_object;
    for (jsize k = 0; k < $1_fields_size; ++k) {
        auto field_ptr = ($1_object).Add();

        jobject obj = jenv->GetObjectArrayElement($input, k);
        if (k == 0) {
            jclass clazz = jenv->GetObjectClass(obj);
            $1_set_method = jenv->GetMethodID(clazz, "toByteArray", "()[B");
        }

        jbyteArray $1_byte_arr = (jbyteArray) jenv->CallObjectMethod(obj, $1_set_method);
        jsize $1_bytes_size = jenv->GetArrayLength($1_byte_arr);
        jbyte* $1_bytes = (jbyte *)jenv->GetByteArrayElements($1_byte_arr, NULL);
        field_ptr->ParseFromArray($1_bytes, $1_bytes_size);
        jenv->ReleaseByteArrayElements($1_byte_arr, $1_bytes, 0);
    }
    $1 = &($1_object);
%}
%typemap(in) TYPE* %{
    if(!$input) {
        if (!jenv->ExceptionCheck()) {
            SWIG_JavaThrowException(jenv, SWIG_JavaNullPointerException, "null string");
        }
        return $null;
    }
    jsize $1_fields_size = jenv->GetArrayLength($input);

    jmethodID $1_set_method;

    TYPE $1_object;
    for (jsize k = 0; k < $1_fields_size; ++k) {
        auto field_ptr = ($1_object).Add();

        jobject obj = jenv->GetObjectArrayElement($input, k);
        if (k == 0) {
            jclass clazz = jenv->GetObjectClass(obj);
            $1_set_method = jenv->GetMethodID(clazz, "toByteArray", "()[B");
        }

        jbyteArray $1_byte_arr = (jbyteArray) jenv->CallObjectMethod(obj, $1_set_method);
        jsize $1_bytes_size = jenv->GetArrayLength($1_byte_arr);
        jbyte* $1_bytes = (jbyte *)jenv->GetByteArrayElements($1_byte_arr, NULL);
        field_ptr->ParseFromArray($1_bytes, $1_bytes_size);
        jenv->ReleaseByteArrayElements($1_byte_arr, $1_bytes, 0);
    }
    $1 = &($1_object);
%}


%typemap(javain) TYPE "($javainput).toArray()"
%typemap(javain) const TYPE & "($javainput).toArray()"
%typemap(javain) TYPE* "($javainput).toArray()"


%typemap(out) TYPE {
    jsize $1_fields_size = ($1).size();
    $result = jenv->NewObjectArray($1_fields_size, jenv->FindClass("[B"), NULL);
    for (jint k = 0; k < $1_fields_size; ++k) {
        auto& field = ($1).Get(k);
        jsize $1_bytes_size = field.ByteSize();
        jbyteArray $1_byte_arr = jenv->NewByteArray($1_bytes_size);
        jbyte* $1_bytes = (jbyte *)jenv->GetByteArrayElements($1_byte_arr, NULL);
        field.SerializeToArray($1_bytes, $1_bytes_size);
        jenv->ReleaseByteArrayElements($1_byte_arr, $1_bytes, 0);
        jenv->SetObjectArrayElement($result, k, $1_byte_arr);
    }
}
%typemap(out) const TYPE& {
    jsize $1_fields_size = ($1)->size();
    $result = jenv->NewObjectArray($1_fields_size, jenv->FindClass("[B"), NULL);
    for (jint k = 0; k < $1_fields_size; ++k) {
        auto& field = ($1)->Get(k);
        jsize $1_bytes_size = field.ByteSize();
        jbyteArray $1_byte_arr = jenv->NewByteArray($1_bytes_size);
        jbyte* $1_bytes = (jbyte *)jenv->GetByteArrayElements($1_byte_arr, NULL);
        field.SerializeToArray($1_bytes, $1_bytes_size);
        jenv->ReleaseByteArrayElements($1_byte_arr, $1_bytes, 0);
        jenv->SetObjectArrayElement($result, k, $1_byte_arr);
    }
}
%typemap(out) TYPE* {
    jsize $1_fields_size = ($1)->size();
    $result = jenv->NewObjectArray($1_fields_size, jenv->FindClass("[B"), NULL);
    for (jint k = 0; k < $1_fields_size; ++k) {
        auto& field = ($1)->Get(k);
        jsize $1_bytes_size = field.ByteSize();
        jbyteArray $1_byte_arr = jenv->NewByteArray($1_bytes_size);
        jbyte* $1_bytes = (jbyte *)jenv->GetByteArrayElements($1_byte_arr, NULL);
        field.SerializeToArray($1_bytes, $1_bytes_size);
        jenv->ReleaseByteArrayElements($1_byte_arr, $1_bytes, 0);
        jenv->SetObjectArrayElement($result, k, $1_byte_arr);
    }
}

%typemap(javaout) TYPE, const TYPE&, TYPE* {
    java.util.List<JTYPE> list = new java.util.ArrayList();
    Object[] fields = $jnicall;
    if (fields == null) {
        return null;
    }
    try {
        for (Object o : fields) {
            list.add(JTYPE.parseFrom((byte[])o));
        }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        e.printStackTrace();
        return null;
    }
    return list;
}


%enddef

#endif

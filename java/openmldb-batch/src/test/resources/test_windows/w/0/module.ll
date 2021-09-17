; ModuleID = 'sql'
source_filename = "sql"
target datalayout = "e-m:o-i64:64-f80:128-n8:16:32:64-S128"

define i32 @__internal_sql_codegen_4(i64, i8*, i8*, i8*, i8**) {
__fn_entry__:
  %is_null_addr2 = alloca i8, align 1
  %is_null_addr1 = alloca i8, align 1
  %is_null_addr = alloca i8, align 1
  %5 = call i8* @hybridse_storage_get_row_slice(i8* %1, i64 0)
  %6 = call i64 @hybridse_storage_get_row_slice_size(i8* %1, i64 0)
  %7 = call i32 @hybridse_storage_get_int32_field(i8* %5, i32 0, i32 7, i8* nonnull %is_null_addr)
  %8 = load i8, i8* %is_null_addr, align 1
  %9 = call i8* @hybridse_storage_get_row_slice(i8* %1, i64 0)
  %10 = call i64 @hybridse_storage_get_row_slice_size(i8* %1, i64 0)
  %11 = call i64 @hybridse_storage_get_int64_field(i8* %9, i32 1, i32 11, i8* nonnull %is_null_addr1)
  %12 = load i8, i8* %is_null_addr1, align 1
  %13 = call i8* @hybridse_storage_get_row_slice(i8* %1, i64 0)
  %14 = call i64 @hybridse_storage_get_row_slice_size(i8* %1, i64 0)
  %15 = call double @hybridse_storage_get_double_field(i8* %13, i32 2, i32 19, i8* nonnull %is_null_addr2)
  %16 = load i8, i8* %is_null_addr2, align 1
  %malloccall = tail call i8* @malloc(i32 35)
  store i8* %malloccall, i8** %4, align 8
  store i8 1, i8* %malloccall, align 1
  %17 = ptrtoint i8* %malloccall to i64
  %ptr_add_offset3 = add i64 %17, 1
  %18 = inttoptr i64 %ptr_add_offset3 to i8*
  store i8 1, i8* %18, align 1
  %ptr_add_offset4 = add i64 %17, 2
  %19 = inttoptr i64 %ptr_add_offset4 to i32*
  store i32 35, i32* %19, align 4
  %ptr_add_offset5 = add i64 %17, 6
  %20 = inttoptr i64 %ptr_add_offset5 to i8*
  store i8 0, i8* %20, align 1
  %21 = shl i8 %8, 7
  %sext = ashr exact i8 %21, 7
  call void @hybridse_storage_encode_nullbit(i8* %malloccall, i32 0, i8 %sext)
  %ptr_add_offset6 = add i64 %17, 7
  %22 = inttoptr i64 %ptr_add_offset6 to i32*
  store i32 %7, i32* %22, align 4
  %23 = shl i8 %12, 7
  %sext9 = ashr exact i8 %23, 7
  call void @hybridse_storage_encode_nullbit(i8* %malloccall, i32 1, i8 %sext9)
  %ptr_add_offset7 = add i64 %17, 11
  %24 = inttoptr i64 %ptr_add_offset7 to i64*
  store i64 %11, i64* %24, align 8
  %25 = shl i8 %16, 7
  %sext10 = ashr exact i8 %25, 7
  call void @hybridse_storage_encode_nullbit(i8* %malloccall, i32 2, i8 %sext10)
  %ptr_add_offset8 = add i64 %17, 19
  %26 = inttoptr i64 %ptr_add_offset8 to double*
  store double %15, double* %26, align 8
  %27 = load i8*, i8** %4, align 8
  call void @__internal_sql_codegen_4_multi_column_agg_1__(i8* %2, i8* %27)
  ret i32 0
}

declare i8* @hybridse_storage_get_row_slice(i8*, i64)

declare i64 @hybridse_storage_get_row_slice_size(i8*, i64)

declare i32 @hybridse_storage_get_int32_field(i8*, i32, i32, i8*)

declare i64 @hybridse_storage_get_int64_field(i8*, i32, i32, i8*)

declare double @hybridse_storage_get_double_field(i8*, i32, i32, i8*)

declare noalias i8* @malloc(i32)

; Function Attrs: argmemonly nounwind
declare void @llvm.memset.p0i8.i32(i8* nocapture writeonly, i8, i32, i1 immarg) #0

declare void @hybridse_storage_encode_nullbit(i8*, i32, i8)

define void @__internal_sql_codegen_4_multi_column_agg_1__(i8*, i8*) {
head:
  %is_null_addr = alloca i8, align 1
  %row_iter1 = alloca [8 x i8], align 1
  %sum = alloca double, align 8
  %row_iter1.sub = getelementptr inbounds [8 x i8], [8 x i8]* %row_iter1, i64 0, i64 0
  store double 0.000000e+00, double* %sum, align 8
  call void @hybridse_storage_get_row_iter(i8* %0, i8* nonnull %row_iter1.sub)
  br label %enter_iter

enter_iter:                                       ; preds = %iter_body, %head
  %2 = phi double [ %12, %iter_body ], [ 0.000000e+00, %head ]
  %3 = phi i64 [ %13, %iter_body ], [ 0, %head ]
  %4 = call i1 @hybridse_storage_row_iter_has_next(i8* nonnull %row_iter1.sub)
  br i1 %4, label %iter_body, label %exit_iter

iter_body:                                        ; preds = %enter_iter
  %5 = call i8* @hybridse_storage_row_iter_get_cur_slice(i8* nonnull %row_iter1.sub, i64 0)
  %6 = call i64 @hybridse_storage_row_iter_get_cur_slice_size(i8* nonnull %row_iter1.sub, i64 0)
  %7 = call double @hybridse_storage_get_double_field(i8* %5, i32 2, i32 19, i8* nonnull %is_null_addr)
  %8 = load i8, i8* %is_null_addr, align 1
  %9 = and i8 %8, 1
  %10 = icmp eq i8 %9, 0
  %11 = fadd double %7, %2
  %12 = select i1 %10, double %11, double %2
  store double %12, double* %sum, align 8
  call void @hybridse_storage_row_iter_next(i8* nonnull %row_iter1.sub)
  %13 = bitcast double %12 to i64
  br label %enter_iter

exit_iter:                                        ; preds = %enter_iter
  call void @hybridse_storage_row_iter_delete(i8* nonnull %row_iter1.sub)
  %14 = bitcast double* %sum to i64*
  %15 = ptrtoint i8* %1 to i64
  %ptr_add_offset = add i64 %15, 27
  %16 = inttoptr i64 %ptr_add_offset to i64*
  store i64 %3, i64* %16, align 8
  ret void
}

declare void @hybridse_storage_get_row_iter(i8*, i8*)

declare i1 @hybridse_storage_row_iter_has_next(i8*)

declare i8* @hybridse_storage_row_iter_get_cur_slice(i8*, i64)

declare i64 @hybridse_storage_row_iter_get_cur_slice_size(i8*, i64)

declare void @hybridse_storage_row_iter_next(i8*)

declare void @hybridse_storage_row_iter_delete(i8*)

define i32 @__internal_sql_codegen_5(i64, i8*, i8*, i8*, i8**) {
__fn_entry__:
  %is_null_addr = alloca i8, align 1
  %5 = call i8* @hybridse_storage_get_row_slice(i8* %1, i64 0)
  %6 = call i64 @hybridse_storage_get_row_slice_size(i8* %1, i64 0)
  %7 = call i32 @hybridse_storage_get_int32_field(i8* %5, i32 0, i32 7, i8* nonnull %is_null_addr)
  %8 = load i8, i8* %is_null_addr, align 1
  %malloccall = tail call i8* @malloc(i32 11)
  store i8* %malloccall, i8** %4, align 8
  store i8 1, i8* %malloccall, align 1
  %9 = ptrtoint i8* %malloccall to i64
  %ptr_add_offset1 = add i64 %9, 1
  %10 = inttoptr i64 %ptr_add_offset1 to i8*
  store i8 1, i8* %10, align 1
  %ptr_add_offset2 = add i64 %9, 2
  %11 = inttoptr i64 %ptr_add_offset2 to i32*
  store i32 11, i32* %11, align 4
  %ptr_add_offset3 = add i64 %9, 6
  %12 = inttoptr i64 %ptr_add_offset3 to i8*
  store i8 0, i8* %12, align 1
  %13 = shl i8 %8, 7
  %sext = ashr exact i8 %13, 7
  call void @hybridse_storage_encode_nullbit(i8* %malloccall, i32 0, i8 %sext)
  %ptr_add_offset4 = add i64 %9, 7
  %14 = inttoptr i64 %ptr_add_offset4 to i32*
  store i32 %7, i32* %14, align 4
  ret i32 0
}

define i32 @__internal_sql_codegen_6(i64, i8*, i8*, i8*, i8**) {
__fn_entry__:
  %is_null_addr = alloca i8, align 1
  %5 = call i8* @hybridse_storage_get_row_slice(i8* %1, i64 0)
  %6 = call i64 @hybridse_storage_get_row_slice_size(i8* %1, i64 0)
  %7 = call i64 @hybridse_storage_get_int64_field(i8* %5, i32 1, i32 11, i8* nonnull %is_null_addr)
  %8 = load i8, i8* %is_null_addr, align 1
  %malloccall = tail call i8* @malloc(i32 15)
  store i8* %malloccall, i8** %4, align 8
  store i8 1, i8* %malloccall, align 1
  %9 = ptrtoint i8* %malloccall to i64
  %ptr_add_offset1 = add i64 %9, 1
  %10 = inttoptr i64 %ptr_add_offset1 to i8*
  store i8 1, i8* %10, align 1
  %ptr_add_offset2 = add i64 %9, 2
  %11 = inttoptr i64 %ptr_add_offset2 to i32*
  store i32 15, i32* %11, align 4
  %ptr_add_offset3 = add i64 %9, 6
  %12 = inttoptr i64 %ptr_add_offset3 to i8*
  store i8 0, i8* %12, align 1
  %13 = shl i8 %8, 7
  %sext = ashr exact i8 %13, 7
  call void @hybridse_storage_encode_nullbit(i8* %malloccall, i32 0, i8 %sext)
  %ptr_add_offset4 = add i64 %9, 7
  %14 = inttoptr i64 %ptr_add_offset4 to i64*
  store i64 %7, i64* %14, align 8
  ret i32 0
}

define i32 @__internal_sql_codegen_7(i64, i8*, i8*, i8*, i8**) {
__fn_entry__:
  %is_null_addr = alloca i8, align 1
  %5 = call i8* @hybridse_storage_get_row_slice(i8* %1, i64 0)
  %6 = call i64 @hybridse_storage_get_row_slice_size(i8* %1, i64 0)
  %7 = call i64 @hybridse_storage_get_int64_field(i8* %5, i32 1, i32 11, i8* nonnull %is_null_addr)
  %8 = load i8, i8* %is_null_addr, align 1
  %malloccall = tail call i8* @malloc(i32 15)
  store i8* %malloccall, i8** %4, align 8
  store i8 1, i8* %malloccall, align 1
  %9 = ptrtoint i8* %malloccall to i64
  %ptr_add_offset1 = add i64 %9, 1
  %10 = inttoptr i64 %ptr_add_offset1 to i8*
  store i8 1, i8* %10, align 1
  %ptr_add_offset2 = add i64 %9, 2
  %11 = inttoptr i64 %ptr_add_offset2 to i32*
  store i32 15, i32* %11, align 4
  %ptr_add_offset3 = add i64 %9, 6
  %12 = inttoptr i64 %ptr_add_offset3 to i8*
  store i8 0, i8* %12, align 1
  %13 = shl i8 %8, 7
  %sext = ashr exact i8 %13, 7
  call void @hybridse_storage_encode_nullbit(i8* %malloccall, i32 0, i8 %sext)
  %ptr_add_offset4 = add i64 %9, 7
  %14 = inttoptr i64 %ptr_add_offset4 to i64*
  store i64 %7, i64* %14, align 8
  ret i32 0
}

attributes #0 = { argmemonly nounwind }

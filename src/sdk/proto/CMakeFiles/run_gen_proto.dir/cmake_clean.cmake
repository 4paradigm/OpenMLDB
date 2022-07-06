file(REMOVE_RECURSE
  "CMakeFiles/run_gen_proto"
  "batch.pb.cc"
  "dbms.pb.cc"
  "fe_common.pb.cc"
  "fe_tablet.pb.cc"
  "fe_type.pb.cc"
  "plan.pb.cc"
)

# Per-language clean rules from dependency scanning.
foreach(lang )
  include(CMakeFiles/run_gen_proto.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()

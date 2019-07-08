#! /bin/sh
#
# release.sh
#
#set -x
java_vesrion_file="./java/src/main/java/com/_4paradigm/rtidb/client/Version.java"
cmake_file="./CMakeLists.txt"
i=1
while((1==1))
do
    value=`echo $1|cut -d "." -f$i`
    if [ "$value" != "" ]
    then
        case "$i" in 
            "1")
                sed -i 's/MAJOR =.*/MAJOR = '${value}\;'/g' ${java_vesrion_file}
                sed -i 's/RTIDB_VERSION_MAJOR .*/RTIDB_VERSION_MAJOR '${value}')/g' ${cmake_file};;
            "2")
                sed -i 's/MEDIUM =.*/MEDIUM = '${value}\;'/g' ${java_vesrion_file}
                sed -i 's/RTIDB_VERSION_MEDIUM .*/RTIDB_VERSION_MEDIUM '${value}')/g' ${cmake_file};;
            "3")
                sed -i 's/MINOR =.*/MINOR = '${value}\;'/g' ${java_vesrion_file}
                sed -i 's/RTIDB_VERSION_MINOR .*/RTIDB_VERSION_MINOR '${value}')/g' ${cmake_file};;
            "4")
                sed -i 's/BUG =.*/BUG = '${value}\;'/g' ${java_vesrion_file}
                sed -i 's/RTIDB_VERSION_BUG .*/RTIDB_VERSION_BUG '${value}')/g' ${cmake_file};;
            *)
                echo "xx";;
        esac        
        ((i++))
    else
        break
    fi
done

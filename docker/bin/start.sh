#! /bin/sh
#
# start.sh
if [ -z "${ROLE}" ]; then
    echo "must set role, role list: blob_proxy"
    exit 1
fi

SELFDIR=$(dirname $0)
WORKDIR=$(dirname ${SELFDIR})
cd ${WORKDIR}
case "${ROLE}" in
    'blob_proxy')
        ./bin/mon ./bin/boot_blob_proxy.sh
        ;;
    *)
        echo "muset set role"
        exit 1
        ;;
esac

#!/bin/sh
# from https://gitlab.4pd.io/inf/base-images/blob/build/shells/push_meta
set +e

skipDeploy=0
if  [[ "$1"  == "skipDeploy" ]];then
    skipDeploy=1
fi

docker_prefix=docker-search.4pd.io
push_docker_prefix=docker.4pd.io
prefix=https://nexus.4pd.io/repository/prophet-meta
# guess module name from CI_PROJECT_NAME
if [[ 'x'${module} == 'x' ]]; then
    module="${CI_PROJECT_NAME}"
fi
# guess tag from CI_JOB_ID
if [[ 'x'${tag} == 'x' ]]; then
    tag=pipe-$(echo "${CI_PIPELINE_IID}" | sed 's/(//g' | sed 's/)//g')-commit-${CI_COMMIT_SHA:0:8}
fi
# guess env from CI_COMMIT_REF_NAME
if [[ 'x'${env} == 'x' ]]; then
    env=$(echo ${CI_COMMIT_REF_NAME} | tr '[A-Z]' '[a-z]')
fi

tag_prefix=env/${env}/meta/${tag}/${module}
lat_prefix=env/${env}/meta/latest/${module}
meta_loc=${tag_prefix}/${module}.k8s.meta
conf_loc=${tag_prefix}/${module}.meta.tar.gz
link_loc=${lat_prefix}/link
pack_loc=${docker_prefix}/env/${env}/prophet/app/${module}.tar:${tag}
push_pack_loc=${push_docker_prefix}/env/${env}/prophet/app/${module}.tar:${tag}

function dockerid()
{
    docker images --format "{{.ID}} {{.Repository}}:{{.Tag}}"|grep $push_pack_loc|awk '{print $1}'
}

function dockersize()
{
    docker inspect $push_pack_loc -f '{{.Size}}'
}

function gen_meta()
{
    target=$1
    id=$( dockerid )
    size=$( dockersize )
    echo "name: ${module}" >> $target
    echo "version: ${tag}" >> $target
    echo "location: ${pack_loc}" >> $target
    echo "conf_location: ${conf_loc}" >> $target
    echo "repositoryUrl: ${CI_PROJECT_PATH}" >> $target
    echo "size: ${size}" >> $target
    echo "md5: ${id}" >> $target
}

function gen_link()
{
    echo -n "${tag}" > $1
}

lastversion=$(curl -s ftp://ftp.4pd.io/pub/i18n/${env}/latest)
if [ $? -eq 0 ]
then
    wget -nH -m -e robots=off -np ftp://ftp.4pd.io/pub/i18n/${env}/${lastversion}/${module} --cut-dirs=5 -P i18nresource
    if [ $? -eq 0 ]
    then
        if [ -d i18nresource/config_center ]
        then
            mkdir -p .add_config_center_i18n_resource
            tar xf ${module}.meta.tar.gz -C .add_config_center_i18n_resource
            cp -r i18nresource/config_center .add_config_center_i18n_resource/META-INFO/
            tar czvf ${module}.meta.tar.gz -C .add_config_center_i18n_resource META-INFO/
        fi
    fi
fi


mkdir -p .ci_push_temp
cp ${module}.meta.tar.gz .ci_push_temp/
gen_link ./.ci_push_temp/link
gen_meta ./.ci_push_temp/${module}.k8s.meta

cd .ci_push_temp/
bash upload-meta-to-nexus 'link' $lat_prefix
bash upload-meta-to-nexus "${module}.k8s.meta" $tag_prefix
if [ $skipDeploy -eq 0 ];then
    bash upload-meta-to-nexus "${module}.meta.tar.gz" $tag_prefix
fi
cd ..
rm -rf .ci_push_temp



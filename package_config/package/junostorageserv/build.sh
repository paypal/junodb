#! /bin/bash

pkg_dir=${1:-$BUILDTOP/package_config/package/junostorageserv}
pkg_basename=$(basename $pkg_dir)

if [[ "$JUNO_BUILD_DIR" == "" ]]; then
  echo "JUNO_BUILD_DIR required but not defined"
  exit
fi

if [[ ! -d "$JUNO_BUILD_DIR" ]]; then
  echo "$JUNO_BUILD_DIR not exist"
  exit
fi


function generate_config() {
  local config_chain

  cfg=$pkg_dir/config/config.toml
  if [[ -f "$cfg" ]]; then
      config_chain+=" $cfg"
  fi  

  #echo $config_chain
  local conf_file

  conf_file=config-$pkg_basename.toml 
  $JUNO_BUILD_DIR/junocli config -o $pkg_dir/$conf_file $config_chain
}

generate_config

#!/bin/bash
# include this file like:
#     # shellcheck disable=SC1090
#     source "$(dirname "$0")/..relpath../dev/include.sh" #follow
# make sure the `..relpath..` is a path relative to the script
# the #follow ensures the auto-help picks up any variables or tasks in the sourced file

if [ -n "$DEBUG_RUN" ]; then
  set -x
fi

set -e -o pipefail
orig_args=("$@")
scriptpath="${BASH_SOURCE[1]}"
script="$(basename "$scriptpath")"
rootdir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)"

cd "$( dirname "$scriptpath" )" >/dev/null 2>&1

function cmd {
  echo $'\e[0;37m>' "$*" $'\e[0m'
  command "$@"
  echo
}

function info {
  echo $'\e[1;37m•' "$*" $'\e[0m'
}

function task_exists {
  [ "$(type -t "$1")" == "function" ]
}

# To be invoked by callers like `main "$@"`
function main {
  # find include files (probably this one)
  read -ra files < <(sed -En 's/source "\$\(dirname "\$0")\/(.+)".*#follow/\1/p' "$script")
  # echo "${files[@]}"

  if [[ "$1" == "-t" ]]; then
    task_exists "$2"
  elif [[ $# -eq 0 || "$1" == "help" || "$1" == "--help" ]]; then
    if task_exists "$2"; then
      type "$2"
    else
      echo "Commands:"
      sed -En 's/^function (.*) \{ #/  \1/p' "$script" "${files[@]}"
      echo
      echo "Environment Variables:"
      sed -En 's/^: "\$\{(.+):=(.+)\}"( #)?/  \1=\2\t/p' "$script" "${files[@]}"
      if [[ "$(type -t _help_post)" == "function" ]]; then
        echo
        _help_post
      fi
    fi
  else
    "$@"
  fi
}

: "${USE_DEV_DOCKER:=yes}" # if no, don't invoke the command inside the dev docker container
: "${FORCE_DOCKER_REBUILD:=no}" # if yes, rebuild the dev docker image

dev_img="dbsteward-dev-docker:1"
function run-in-docker {
  if [[ $USE_DEV_DOCKER == "yes" ]]; then
    build-dev-docker
    script="$(basename "$scriptpath")"
    info "Running './$script ${orig_args[*]}' in development container"

    docker_args=(
      --rm
      --network host
      -e USE_DEV_DOCKER=no
      -e DEBUG_RUN
      -w /home/dev
      -v "$(pwd):/home/dev"
      -v /var/run/docker.sock:/var/run/docker.sock
      -e GOFILE -e GOPACKAGE
      -v "${GOPATH:-$HOME/go}/pkg/mod:/go/pkg/mod"
    )
    
    exec docker run "${docker_args[@]}" "$@" "$dev_img" "./$script" "${orig_args[@]}"
  fi
}

function build-dev-docker {
  if [[ $FORCE_DOCKER_REBUILD == yes || -z "$(docker images -q "$dev_img")" ]]; then
    info "Building dev docker image"
    docker build -f "$rootdir/dev/dev.Dockerfile" -t "$dev_img" .
  fi
}

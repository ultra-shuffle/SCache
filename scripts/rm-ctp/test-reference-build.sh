#!/usr/bin/env bash
# Real compiler test only. Reference UMDK is NOT a vendor/hardware validation.
set -Eeuo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd -P)
: "${RM_CTP_PACKAGE:?}" "${URMA_INCLUDE:?}" "${URMA_LIB:?}" "${URMA_COMMON_LIB:?}"
WORK=${1:?absolute test workspace}
mkdir -p "$WORK"
VIEW=$(mktemp -d "$WORK/reference-view-XXXXXXXX")
mkdir -p "$VIEW/bin" "$VIEW/sources/scache-jni"
ln -s "$RM_CTP_PACKAGE/dependencies" "$VIEW/dependencies"
cp "$RM_CTP_PACKAGE/bin/build-lib-spark-urma.sh" "$VIEW/bin/"
ln -s "$REPO/native/spark_urma/src" "$VIEW/sources/scache-jni/src"
ln -s "$REPO/native/spark_urma/include" "$VIEW/sources/scache-jni/include"
printf 'cmake_minimum_required(VERSION 3.16)\nproject(rm_ctp_package LANGUAGES C CXX)\nset(URMA_INC [==[%s]==] CACHE PATH "Selected public SDK" FORCE)\ninclude([==[%s]==])\n' \
 "$URMA_INCLUDE" "$REPO/native/spark_urma/CMakeLists.txt" > "$VIEW/sources/scache-jni/CMakeLists.txt"
bash "$VIEW/bin/build-lib-spark-urma.sh" --workspace "$VIEW/output" \
 --urma-lib "$URMA_LIB" --urma-common-lib "$URMA_COMMON_LIB" --urma-include "$URMA_INCLUDE" \
 --umdk-label reference-software-test-NOT-vendor
echo "REFERENCE_BUILD_TEST_PASS view=$VIEW (not hardware)"

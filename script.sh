#!/bin/bash

: "${TAG:?}" "${BUILD_IMAGE:?}"
: "${DOCKER_USR:?}" "${DOCKER_PSW:?}" "${ARTIFACTORY_USR:?}" "${ARTIFACTORY_PSW:?}"
printf "%s" "$DOCKER_PSW" | sudo docker login -u "$DOCKER_USR" --password-stdin dockerhub.cisco.com || exit 1

REGISTRY=dockerhub.cisco.com/robot-dockerprod
HELM_URL=https://engci-maven.cisco.com/artifactory/robot-dev-snapshot/robot-base-vm/vgoogle

echo "Tag = $TAG"

# The operator image bakes templates/csi.yaml.j2; kadalu-csi uses the same tag as the operator.
build_operator() {
  sed -i "s|###tag###|$TAG|" templates/csi.yaml.j2
  docker build -f kadalu_operator/Dockerfile -t $REGISTRY/kadalu-operator:$TAG . || exit 1
  docker push $REGISTRY/kadalu-operator:$TAG || exit 1
}

# Chart version drives the image tag used by the helm charts (common.version).
publish_helm() {
  grep -rl '^version: 0.0.0-0' helm --include=Chart.yaml | xargs sed -i "s/^version: 0.0.0-0/version: $TAG/"
  tar -cvf kadalu-helm-$TAG.tar -C helm kadalu/ || exit 1
  curl --noproxy cisco.com -f -i -X PUT -u "$ARTIFACTORY_USR:$ARTIFACTORY_PSW" -C - -T kadalu-helm-$TAG.tar "$HELM_URL/kadalu-helm-$TAG.tar" || exit 1
  echo "Uploaded kadalu-helm-$TAG.tar"
}

if [ "$BUILD_IMAGE" == "KADALU_OPERATOR" ]
then
  build_operator
  publish_helm
elif [ "$BUILD_IMAGE" == "KADALU_CSI_DRIVER" ]
then
  docker build -f csi/Dockerfile -t $REGISTRY/kadalu-csi:$TAG . || exit 1
  docker push $REGISTRY/kadalu-csi:$TAG || exit 1
  build_operator
  publish_helm
else
  echo "Unknown BUILD_IMAGE: $BUILD_IMAGE"
  exit 1
fi

FROM fedora:33

# install needed rpms
RUN INSTALL_PKGS=" \
	hostname kubernetes-client tcpdump procps  \
        " && \
	dnf install --best --refresh -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
	dnf clean all && rm -rf /var/cache/dnf/*

RUN mkdir -p /var/run/openvswitch

COPY scripts/etcd /root
COPY scripts/etcdctl /root
COPY dist/ovn-kubernetes/dist/images/ /root/
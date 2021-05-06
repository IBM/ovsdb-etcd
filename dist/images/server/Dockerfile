FROM fedora:33

# install needed rpms
RUN INSTALL_PKGS=" \
	hostname kubernetes-client tcpdump procps  \
        " && \
	dnf install --best --refresh -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
	dnf clean all && rm -rf /var/cache/dnf/*

COPY pkg/cmd/server/server /root
RUN mkdir -p /root/etcd-ovsdb/schemas
COPY schemas/ /root/etcd-ovsdb/schemas/
COPY dist/ovn-kubernetes/dist/images/ /root/
COPY dist/images/server/ovsdb-client /usr/local/bin

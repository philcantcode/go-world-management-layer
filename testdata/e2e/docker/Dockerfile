FROM scratch
COPY --chmod=0555 testdata/e2e/build/linux-amd64/world-guest /usr/local/bin/world-guest
COPY --chmod=0555 testdata/e2e/build/linux-amd64/world-idle /usr/local/bin/world-idle
ENTRYPOINT ["/usr/local/bin/world-guest"]

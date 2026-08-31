# The default target remains the lightweight image used by CI. The optional
# integration targets add the services and ADIOS2 features needed to test
# remote campaign replicas on a workstation.
FROM python:3.13-slim AS app-deps

WORKDIR /app

RUN pip install --no-cache-dir poetry poetry-plugin-export

COPY pyproject.toml poetry.lock ./

RUN poetry export -f requirements.txt -o requirements.txt --without-hashes --with dev \
    && pip install --no-cache-dir -r requirements.txt \
    && rm requirements.txt

RUN apt-get update \
    && apt-get install --yes --no-install-recommends tk \
    && rm -rf /var/lib/apt/lists/*


FROM app-deps AS app

COPY . .

RUN pip install --no-cache-dir --no-deps .


FROM python:3.13-slim AS integration-secrets

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openssh-client openssl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/integration-secrets \
    && ssh-keygen -q -t ed25519 -N "" -f /run/integration-secrets/client_key \
    && openssl req -x509 -nodes -days 3650 -newkey rsa:2048 \
        -keyout /run/integration-secrets/https.key \
        -out /run/integration-secrets/https.crt \
        -subj "/CN=https-service.docker.hpc-campaign" \
        -addext "subjectAltName=DNS:https-service.docker.hpc-campaign,DNS:localhost,IP:127.0.0.1"


FROM app-deps AS integration-tools

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir boto3

COPY --from=integration-secrets /run/integration-secrets/client_key /run/ssh/client_key
COPY --from=integration-secrets /run/integration-secrets/client_key.pub /run/ssh/client_key.pub
COPY --from=integration-secrets /run/integration-secrets/https.crt /usr/local/share/ca-certificates/hpc-campaign-test.crt
COPY dockerfiles/integration/config.yaml /root/.config/hpc-campaign/config.yaml
COPY dockerfiles/integration/hosts.yaml /root/.config/hpc-campaign/hosts.yaml

RUN chmod 600 /run/ssh/client_key \
    && update-ca-certificates

ENV AWS_CONFIG_FILE=/root/.aws/config \
    AWS_SHARED_CREDENTIALS_FILE=/root/.aws/credentials \
    AWS_EC2_METADATA_DISABLED=true \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

COPY dockerfiles/integration/aws-config /root/.aws/config
COPY dockerfiles/integration/aws-credentials /root/.aws/credentials


FROM integration-tools AS integration-setup

COPY . .

RUN pip install --no-cache-dir --no-deps .


# PyPI wheels intentionally have a smaller feature set and cannot exercise
# HDF5 or S3 campaign replicas. Build a complete client only for integration.
FROM integration-tools AS integration

ARG AWS_SDK_CPP_VERSION=1.11.311
ARG BUILD_JOBS=8

ENV AWS_SDK_PREFIX=/opt/aws/aws-sdk-s3-1.11.311 \
    CMAKE_PREFIX_PATH=/opt/aws/aws-sdk-s3-1.11.311 \
    LD_LIBRARY_PATH=/opt/aws/aws-sdk-s3-1.11.311/lib

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        bison \
        build-essential \
        cmake \
        flex \
        git \
        libcurl4-openssl-dev \
        libffi-dev \
        libhdf5-dev \
        libsodium-dev \
        libsqlite3-dev \
        libssl-dev \
        libxrootd-dev \
        ninja-build \
        pkg-config \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --recurse-submodules --depth 1 --branch "${AWS_SDK_CPP_VERSION}" \
        https://github.com/aws/aws-sdk-cpp.git /tmp/aws-sdk-cpp \
    && cmake -S /tmp/aws-sdk-cpp -B /tmp/aws-sdk-cpp-build -G Ninja \
        -DAUTORUN_UNIT_TESTS=OFF \
        -DBUILD_ONLY="s3;iam;sts;s3-crt" \
        -DBUILD_SHARED_LIBS=ON \
        -DCMAKE_BUILD_TYPE=Debug \
        -DCMAKE_INSTALL_PREFIX="${AWS_SDK_PREFIX}" \
        -DENABLE_TESTING=OFF \
    && cmake --build /tmp/aws-sdk-cpp-build --parallel "${BUILD_JOBS}" \
    && cmake --install /tmp/aws-sdk-cpp-build \
    && rm -rf /tmp/aws-sdk-cpp /tmp/aws-sdk-cpp-build

# Debian packages the XRootD SSI headers and libraries separately from its
# public client SDK.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libxrootd-private-dev \
        libxrootd-server-dev \
    && rm -rf /var/lib/apt/lists/*

# ``adios2-source`` is a named BuildKit context. Copy only build inputs so a
# local checkout can contain large build trees without sending them to Docker.
COPY --from=adios2-source CMakeLists.txt /tmp/adios2/CMakeLists.txt
COPY --from=adios2-source CTestConfig.cmake /tmp/adios2/CTestConfig.cmake
COPY --from=adios2-source CTestCustom.cmake.in /tmp/adios2/CTestCustom.cmake.in
COPY --from=adios2-source bindings /tmp/adios2/bindings
COPY --from=adios2-source cmake /tmp/adios2/cmake
COPY --from=adios2-source plugins /tmp/adios2/plugins
COPY --from=adios2-source python /tmp/adios2/python
COPY --from=adios2-source source /tmp/adios2/source
COPY --from=adios2-source thirdparty /tmp/adios2/thirdparty

RUN cmake -S /tmp/adios2 -B /tmp/adios2-build -G Ninja \
        -DADIOS2_BUILD_EXAMPLES=OFF \
        -DADIOS2_USE_AWSSDK=ON \
        -DADIOS2_USE_CURL=ON \
        -DADIOS2_USE_Campaign=ON \
        -DADIOS2_USE_Fortran=OFF \
        -DADIOS2_USE_HDF5=ON \
        -DADIOS2_USE_MPI=OFF \
        -DADIOS2_USE_Python=ON \
        -DADIOS2_USE_Sodium=ON \
        -DADIOS2_USE_XRootD=ON \
        -DBUILD_TESTING=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_INSTALL_PYTHONDIR=/usr/local/lib/python3.13/site-packages \
        -DPython_EXECUTABLE=/usr/local/bin/python \
    && cmake --build /tmp/adios2-build --parallel "${BUILD_JOBS}" \
    && pip uninstall --yes adios2 \
    && cmake --install /tmp/adios2-build \
    && ldconfig \
    && rm -rf /tmp/adios2 /tmp/adios2-build \
    && python -c "import adios2; print(adios2.__version__)"

COPY . .

RUN pip install --no-cache-dir --no-deps .

ENTRYPOINT ["/app/dockerfiles/integration/runner-entrypoint.sh"]
CMD ["bash"]


FROM integration AS integration-development

RUN apt-get update \
    && apt-get install --yes --no-install-recommends gdb procps \
    && rm -rf /var/lib/apt/lists/*

COPY dockerfiles/integration/build-adios2-dev.sh /usr/local/bin/build-adios2-dev

RUN chmod +x /usr/local/bin/build-adios2-dev

ENV ADIOS2_DEV_SOURCE_DIR=/workspace/ADIOS2 \
    ADIOS2_DEV_BUILD_DIR=/workspace/ADIOS2-build \
    ADIOS2_DEV_INSTALL_PREFIX=/opt/adios2-dev \
    PATH=/opt/adios2-dev/bin:$PATH \
    PYTHONPATH=/workspace/hpc-campaign:/opt/adios2-dev/lib/python3.13/site-packages \
    CMAKE_PREFIX_PATH=/opt/adios2-dev:$CMAKE_PREFIX_PATH \
    LD_LIBRARY_PATH=/opt/adios2-dev/lib:$LD_LIBRARY_PATH

WORKDIR /workspace/hpc-campaign


FROM app AS https-service

COPY --from=integration-secrets /run/integration-secrets/https.crt /run/https/https.crt
COPY --from=integration-secrets /run/integration-secrets/https.key /run/https/https.key

ENTRYPOINT ["python", "/app/dockerfiles/integration/https_server.py"]
CMD ["--directory", "/srv", "--port", "443", "--certificate", "/run/https/https.crt", "--key", "/run/https/https.key"]


FROM integration AS ssh-service

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openssh-server \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash campaign \
    && echo 'campaign:hpc-campaign-test-only' | chpasswd \
    && mkdir -p /home/campaign/.ssh /run/sshd \
    && ssh-keygen -A

COPY --from=integration-secrets /run/integration-secrets/client_key.pub /home/campaign/.ssh/authorized_keys

RUN chown -R campaign:campaign /home/campaign/.ssh \
    && chmod 700 /home/campaign/.ssh \
    && chmod 600 /home/campaign/.ssh/authorized_keys

ENTRYPOINT ["/usr/sbin/sshd"]
CMD ["-D", "-e", "-o", "PasswordAuthentication=no", "-o", "PermitRootLogin=no", "-o", "PubkeyAuthentication=yes", "-o", "AllowUsers=campaign"]


# Keep this last so a plain `docker build -f dockerfiles/Dockerfile .` has the
# same lightweight result as the existing CI workflow.
FROM app AS ci
